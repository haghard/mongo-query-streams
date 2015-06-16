/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mongo

import org.apache.log4j.Logger
import scalaz.concurrent.Task
import scalaz.{ -\/, \/, \/- }
import scala.util.{ Failure, Success, Try }
import scalaz.stream._
import java.util.concurrent.{ ExecutorService, TimeUnit }
import com.mongodb.{ DBObject, MongoClient, MongoException }

package object query {
  import scalaz.Scalaz._
  import scalaz.stream.Process._
  import scalaz.stream.process1._

  type MongoChannel[T, A] = Channel[Task, T, Process[Task, A]]
  type MChannel[A] = MongoChannel[MongoClient, A]

  private[mongo] case class QuerySetting(q: DBObject, db: String, collName: String, sortQuery: Option[DBObject],
                                         limit: Option[Int], skip: Option[Int], maxTimeMS: Option[Long])

  private[mongo] trait MongoStreamFactory[T] {
    def createMStream(arg: String \/ QuerySetting)(implicit pool: ExecutorService): MongoStream[T, DBObject]
  }

  private[mongo] case class MongoStream[T, A](val out: MongoChannel[T, A]) {

    private def resultMap[B](f: Process[Task, A] ⇒ Process[Task, B]): MongoStream[T, B] =
      MongoStream(out.map(r ⇒ r andThen (pt ⇒ pt.map(p ⇒ f(p)))))

    private def pipe[B](p2: Process1[A, B]): MongoStream[T, B] = resultMap(_.pipe(p2))

    def |>[B](p2: Process1[A, B]): MongoStream[T, B] = pipe(p2)

    /**
     *
     * @param f
     * @tparam B
     * @return
     */
    def map[B](f: A ⇒ B): MongoStream[T, B] = resultMap(_.map(f))

    /**
     *
     * @param f
     * @tparam B
     * @return
     */
    def flatMap[B](f: A ⇒ MongoStream[T, B]): MongoStream[T, B] = MongoStream {
      out.map(
        (g: T ⇒ Task[Process[Task, A]]) ⇒ (task: T) ⇒
          g(task).map { p ⇒
            p.flatMap((a: A) ⇒
              f(a).out.flatMap(h ⇒ eval(h(task)).flatMap(i ⇒ i)))
          }
      )
    }

    /**
     * Interleave or combine the outputs of two processes.
     * If at any point the awaits on a side that has halted, we gracefully kill off the other side.
     * If at any point one terminates with cause `c`, both sides are killed, and
     * the resulting `Process` terminates with `c`.
     * Useful combinator for querying one-to-one relations or just taking first one from the right
     * @param stream
     * @param f
     * @tparam B
     * @tparam C
     * @return MongoStream[T, C]
     */
    def zipWith[B, C](stream: MongoStream[T, B])(implicit f: (A, B) ⇒ C): MongoStream[T, C] = MongoStream {
      val zipper: ((T ⇒ Task[Process[Task, A]], T ⇒ Task[Process[Task, B]]) ⇒ (T ⇒ Task[Process[Task, C]])) = {
        (fa, fb) ⇒
          (r: T) ⇒
            for {
              x ← fa(r)
              y ← fb(r)
            } yield x.zipWith(y)(f)
      }

      def deterministicZip[I, I2, O](f: (I, I2) ⇒ O): Tee[I, I2, O] =
        (for {
          l ← awaitL[I]
          r ← awaitR[I2]
          pair ← emit(f(l, r))
        } yield pair).repeat

      out.tee(stream.out)(deterministicZip(zipper))
    }

    /**
     * Interleave or combine the outputs of two processes.
     * If at any point the awaits on a side that has halted, we gracefully kill off the other side.
     * If at any point one terminates with cause `c`, both sides are killed, and
     * the resulting `Process` terminates with `c`.
     * Useful combinator for querying one-to-one relations or just taking first one from the right
     *
     * @param stream
     * @tparam B
     * @return MongoStream[T, (A, B)]
     */
    def zip[B](stream: MongoStream[T, B]): MongoStream[T, (A, B)] = zipWith(stream)((_, _))

    /**
     * One to many relation powered by `flatMap` with restricted field in output
     *
     * @param relation
     * @tparam E
     * @tparam C
     * @return
     */
    def innerJoin[E, C](relation: A ⇒ MongoStream[T, E])(f: (A, E) ⇒ C): MongoStream[T, C] =
      flatMap { id: A ⇒ relation(id) |> (lift { f(id, _) }) }

    /**
     * One to many relation powered by `flatMap` with raw objects in output
     * @param relation
     * @tparam C
     * @return
     */
    def innerJoinRaw[C](relation: A ⇒ MongoStream[T, A])(f: (A, A) ⇒ C): MongoStream[T, C] =
      flatMap { id: A ⇒ relation(id) |> lift(f(id, _)) }

    /**
     * Allows you to extract specified field from [[DBObject]] by name with type cast
     * @param name field name
     * @tparam B  field type
     * @throws `MongoException` If item is not a `DBObject`.
     * @return `MongoStream[T, B]`
     */
    def column[B](name: String): MongoStream[T, B] = {
      pipe(lift { record ⇒
        record match {
          case r: DBObject ⇒ r.get(name).asInstanceOf[B]
          case other       ⇒ throw new MongoException(s"DBObject expected but found ${other.getClass.getName}")
        }
      })
    }
  }

  private[query] trait MutableBuilder {
    private[query] var skip: Option[Int] = None
    private[query] var limit: Option[Int] = None
    private[query] var maxTimeMS: Option[Long] = None
    private[query] var collectionName: Option[String] = None
    private[query] var query: String \/ Option[DBObject] = \/-(None)
    private[query] var dbName: Option[String] = None
    private[query] var sortQuery: String \/ Option[DBObject] = \/-(None)

    private val parser = mongo.mqlparser.MqlParser()

    private def parse(query: String): String \/ Option[DBObject] = {
      Try(parser.parse(query)) match {
        case Success(q)  ⇒ (\/-(Option(q)))
        case Failure(er) ⇒ -\/(er.getMessage)
      }
    }

    def q(q: String): Unit = query = parse(q)

    def q(q: DBObject): Unit = query = \/-(Option(q))

    def q(qc: QueryBuilder): Unit = query = \/-(Option(qc.q))

    def db(name: String): Unit = dbName = Option(name)

    def sort(q: String): Unit = sortQuery = parse(q)

    def sort(query: QueryBuilder): Unit = sortQuery = \/-(Option(query.q))

    def limit(n: Int): Unit = limit = Some(n)

    def skip(n: Int): Unit = skip = Some(n)

    def maxTimeMS(mills: Long): Unit = maxTimeMS = Some(mills)

    def collection(name: String): Unit = collectionName = Some(name)

    def build(): String \/ QuerySetting
  }

  def create[T](f: MutableBuilder ⇒ Unit)(implicit pool: ExecutorService, q: MongoStreamFactory[T]): MongoStream[T, DBObject] = {
    val builder = new MutableBuilder {
      override def build(): String \/ QuerySetting =
        for {
          qOr ← query
          q ← qOr \/> "Query shouldn't be empty"
          db ← dbName \/> "DB name shouldn't be empty"
          c ← collectionName \/> "Collection name shouldn't be empty"
          s ← sortQuery
        } yield QuerySetting(q, db, c, s, limit, skip, maxTimeMS)
    }
    f(builder)
    q createMStream builder.build
  }

  //default
  implicit object default extends MongoStreamFactory[MongoClient] {
    override def createMStream(arg: String \/ QuerySetting)(implicit pool: ExecutorService): MongoStream[MongoClient, DBObject] = {
      arg match {
        case \/-(setting) ⇒
          MongoStream(eval(Task now { client: MongoClient ⇒
            Task {
              val logger = Logger.getLogger("query")
              scalaz.stream.io.resource(
                Task delay {
                  val collection = client.getDB(setting.db).getCollection(setting.collName)
                  val c = collection.find(setting.q)
                  setting.sortQuery.foreach(c.sort(_))
                  setting.skip.foreach(c.skip(_))
                  setting.limit.foreach(c.limit(_))
                  setting.maxTimeMS.foreach(c.maxTime(_, TimeUnit.MILLISECONDS))
                  logger.debug(s"Cursor: ${c.##} Query: ${setting.q} Sort: ${setting.sortQuery}")
                  c
                })(c ⇒ Task.delay(c.close)) { c ⇒
                  Task.delay {
                    if (c.hasNext) {
                      c.next
                    } else {
                      logger.debug(s"Cursor: ${c.##} is exhausted")
                      throw Cause.Terminated(Cause.End)
                    }
                  }
                }
            }
          }))

        case -\/(error) ⇒ MongoStream(eval(Task.fail(new MongoException(error))))
      }
    }
  }
}