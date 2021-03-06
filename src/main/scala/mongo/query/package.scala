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

import scalaz.Scalaz._
import scalaz.stream._
import scalaz.concurrent.Task
import org.apache.log4j.Logger
import scalaz.{ -\/, \/, \/- }
import scala.util.{ Failure, Success, Try }
import java.util.concurrent.{ ExecutorService, TimeUnit }
import com.mongodb.{ DBObject, MongoClient, MongoException }
import scalaz.stream.Process._
import scalaz.stream.process1._

package object query {

  type MongoChannel[T, A] = Channel[Task, T, Process[Task, A]]

  case class QuerySetting(q: DBObject, db: String, cName: String, sortQuery: Option[DBObject],
                          limit: Option[Int], skip: Option[Int], maxTimeMS: Option[Long],
                          readPref: Option[ReadPreference])

  trait DBChannelFactory[T] {
    def createChannel(arg: String \/ QuerySetting)(implicit pool: ExecutorService): DBChannel[T, DBObject]
  }

  /**
   *
   * @param out MongoChannel[T, A]
   * @tparam T MongoClient or Db
   * @tparam A DBObject
   */
  case class DBChannel[T, A](out: MongoChannel[T, A]) {

    private def liftP[B](f: Process[Task, A] ⇒ Process[Task, B]): DBChannel[T, B] =
      DBChannel { out.map(step ⇒ step.andThen(task ⇒ task.map(p ⇒ f(p)))) }

    private def pipe[B](p2: Process1[A, B]): DBChannel[T, B] = liftP(_.pipe(p2))

    private[mongo] def |>[B](p2: Process1[A, B]): DBChannel[T, B] = pipe(p2)

    /**
     *
     * @param f
     * @tparam B
     * @return
     */
    def map[B](f: A ⇒ B): DBChannel[T, B] = liftP(_.map(f))

    /**
     *
     * @param f
     * @tparam B
     * @return
     */
    def flatMap[B](f: A ⇒ DBChannel[T, B]): DBChannel[T, B] = DBChannel {
      out.map(
        (step: T ⇒ Task[Process[Task, A]]) ⇒ (task: T) ⇒
          step(task).map { p ⇒
            p.flatMap((a: A) ⇒
              f(a).out.flatMap(h ⇒ eval(h(task)).flatMap(i ⇒ i)))
          }
      )
    }

    /**
     * Interleave outputs of two processes in deterministic fashion.
     * If at any point the awaits on a side that has halted, we gracefully kill off the other side.
     * If at any point one terminates with cause `c`, both sides are killed, and
     * the resulting `Process` terminates with `c`.
     * Useful combinator for querying one-to-one relations or just taking first one from the right
     * @param stream
     * @param f
     * @tparam B
     * @tparam C
     * @return DBChannel[T, C]
     */
    def zipWith[B, C](stream: DBChannel[T, B])(implicit f: (A, B) ⇒ C): DBChannel[T, C] = DBChannel {
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
     *
     * @param other
     * @param t
     * @tparam B
     * @tparam C
     * @return
     */
    def tee[B, C](other: Process[Task, B])(t: Tee[A, B, C]): DBChannel[T, C] =
      liftP { p ⇒ p.tee(other)(t) }

    /**
     *
     *
     * Interleave or combine the outputs of two processes in nondeterministic fashion.
     * It's useful when you want mix results from 2 query stream
     * @param other
     * @tparam B
     * @return
     */
    def either[B](other: Process[Task, B]): DBChannel[T, A \/ B] =
      liftP { p ⇒ p.wye(other)(scalaz.stream.wye.either) }

    /**
     * Interleave or combine the outputs of two processes.
     * If at any point the awaits on a side that has halted, we gracefully kill off the other side.
     * If at any point one terminates with cause `c`, both sides are killed, and
     * the resulting `Process` terminates with `c`.
     * Useful combinator for querying one-to-one relations or just taking first one from the right
     *
     * @param stream
     * @tparam B
     * @return DBChannel[T, (A, B)]
     */
    def zip[B](stream: DBChannel[T, B]): DBChannel[T, (A, B)] = zipWith(stream)((_, _))

    /**
     * Interleave or combine the outputs of two processes in deterministic fashion. It's useful when you want to fetch object
     * and transform each one with result from `other` process, or restrict result size with size of `other` stream
     * @param other
     * @tparam B
     * @return
     */
    def zip[B](other: Process[Task, B]): DBChannel[T, (A, B)] = liftP { p ⇒ (p zip other) }

    /**
     * One to many relation powered by `flatMap` with restricted field in output
     *
     * @param relation
     * @tparam E
     * @tparam C
     * @return
     */
    def innerJoin[E, C](relation: A ⇒ DBChannel[T, E])(f: (A, E) ⇒ C): DBChannel[T, C] =
      flatMap { id: A ⇒ relation(id) |> (lift { f(id, _) }) }

    /**
     * One to many relation powered by `flatMap` with raw objects in output
     * @param relation
     * @tparam C
     * @return
     */
    def innerJoinRaw[C](relation: A ⇒ DBChannel[T, A])(f: (A, A) ⇒ C): DBChannel[T, C] =
      flatMap { id: A ⇒ relation(id) |> lift(f(id, _)) }

    /**
     * Allows you to extract specified field from [[DBObject]] by name with type cast
     * @param name field name
     * @tparam B  field type
     * @throws MongoException If item is not a `DBObject`.
     * @return DBChannel[T, B]
     */
    def column[B](name: String): DBChannel[T, B] = {
      pipe(lift {
        case r: DBObject ⇒ r.get(name).asInstanceOf[B]
        case other       ⇒ throw new MongoException(s"DBObject expected but found ${other.getClass.getName}")
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
    private[query] var readPreference: Option[ReadPreference] = None

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

    def readPreference(r: ReadPreference): Unit = readPreference = Some(r)

    def build(): String \/ QuerySetting
  }

  def create[T](f: MutableBuilder ⇒ Unit)(implicit pool: ExecutorService, factory: DBChannelFactory[T]): DBChannel[T, DBObject] = {
    val builder = new MutableBuilder {
      override def build(): String \/ QuerySetting =
        for {
          qOr ← query
          q ← qOr \/> "Query shouldn't be empty"
          db ← dbName \/> "DB name shouldn't be empty"
          c ← collectionName \/> "Collection name shouldn't be empty"
          s ← sortQuery
        } yield QuerySetting(q, db, c, s, limit, skip, maxTimeMS, readPreference)
    }
    f(builder)
    factory createChannel builder.build
  }

  //default
  implicit object defaultChannel extends DBChannelFactory[MongoClient] {
    override def createChannel(arg: String \/ QuerySetting)(implicit ES: ExecutorService): DBChannel[MongoClient, DBObject] = {
      arg.fold({ error ⇒ DBChannel(eval(Task.fail(new MongoException(error)))) }, { qs ⇒
        DBChannel(eval(Task.now { client: MongoClient ⇒
          Task {
            val logger = Logger.getLogger("mongo-channel")
            scalaz.stream.io.resource(
              Task delay {
                val cursor = client.getDB(qs.db).getCollection(qs.cName).find(qs.q)
                scalaz.syntax.id.ToIdOpsDeprecated(cursor) |> { c ⇒
                  qs.readPref.fold(c)(p ⇒ c.setReadPreference(p.asMongoDbReadPreference))
                  qs.sortQuery.foreach(c.sort)
                  qs.skip.foreach(c.skip)
                  qs.limit.foreach(c.limit)
                  qs.maxTimeMS.foreach(c.maxTime(_, TimeUnit.MILLISECONDS))
                }
                logger.debug(s"Query:[${qs.q}] ReadPrefs:[${cursor.getReadPreference}] Sort:[${qs.sortQuery}] Limit:[${qs.limit}] Skip:[${qs.skip}]")
                cursor
              })(c ⇒ Task.delay(c.close())) { c ⇒
                Task.delay {
                  if (c.hasNext) c.next
                  else throw Cause.Terminated(Cause.End)
                }
              }
          }(ES)
        }))
      })
    }
  }
}