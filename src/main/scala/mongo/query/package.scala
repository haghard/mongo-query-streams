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

  type MongoChannel[A] = Channel[Task, A, Process[Task, DBObject]]

  private[query] case class QuerySetting(q: DBObject, db: String, collName: String, sortQuery: Option[DBObject],
                                         limit: Option[Int], skip: Option[Int], maxTimeMS: Option[Long])

  private[query] trait ToProcess[T] {
    def toProcess(arg: String \/ QuerySetting)(implicit pool: ExecutorService): MongoStream[T, DBObject]
  }

  private[query] case class MongoStream[T, A](val channel: Channel[Task, T, Process[Task, A]]) {

    private def resultMap[B](f: Process[Task, A] ⇒ Process[Task, B]): MongoStream[T, B] =
      MongoStream(channel.map(r ⇒ r andThen (pt ⇒ pt.map(p ⇒ f(p)))))

    def map[B](f: A ⇒ B): MongoStream[T, B] = resultMap(_.map(f))

    def flatMap[B](f: A ⇒ MongoStream[T, B]): MongoStream[T, B] = MongoStream {
      channel.map(
        (g: T ⇒ Task[Process[Task, A]]) ⇒ (task: T) ⇒
          g(task).map { pa ⇒
            pa.flatMap((a: A) ⇒
              f(a).channel.flatMap(h ⇒ eval(h(task)).flatMap(identity)))
          }
      )
    }

    def pipe[B](p2: Process1[A, B]): MongoStream[T, B] = resultMap(_.pipe(p2))

    def |>[B](p2: Process1[A, B]): MongoStream[T, B] = pipe(p2)

    def zipWith[B, C](stream: MongoStream[T, B])(implicit f: (A, B) ⇒ C): MongoStream[T, C] = MongoStream {
      val zipper: ((T ⇒ Task[Process[Task, A]], T ⇒ Task[Process[Task, B]]) ⇒ (T ⇒ Task[Process[Task, C]])) = {
        (fa, fb) ⇒
          (r: T) ⇒
            for {
              x ← fa(r)
              y ← fb(r)
            } yield x.zipWith(y)(f)
      }

      def zipIt[I, I2, O](f: (I, I2) ⇒ O): Tee[I, I2, O] =
        (for {
          l ← awaitL[I]
          r ← awaitR[I2]
          pair ← emit(f(l, r))
        } yield pair).repeat

      //channel.zipWith(stream.channel)(zipper)
      channel.tee(stream.channel)(zipIt(zipper))
    }

    def zip[B](stream: MongoStream[T, B]): MongoStream[T, (A, B)] = zipWith(stream)((_, _))
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

    def q(qc: mongo.dsl.QueryBuilder): Unit = query = \/-(Option(qc.q))

    def db(name: String): Unit = dbName = Option(name)

    def sort(q: String): Unit = sortQuery = parse(q)

    def sort(query: mongo.dsl.QueryBuilder): Unit = sortQuery = \/-(Option(query.q))

    def limit(n: Int): Unit = limit = Some(n)

    def skip(n: Int): Unit = skip = Some(n)

    def maxTimeMS(mills: Long): Unit = maxTimeMS = Some(mills)

    def collection(name: String): Unit = collectionName = Some(name)

    def build(): String \/ QuerySetting
  }

  def query[T](f: MutableBuilder ⇒ Unit)(implicit pool: ExecutorService, q: ToProcess[T]): MongoStream[T, DBObject] = {
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
    q toProcess builder.build
  }

  //default
  implicit object default extends ToProcess[MongoClient] {
    override def toProcess(arg: String \/ QuerySetting)(implicit pool: ExecutorService): MongoStream[MongoClient, DBObject] = {
      arg match {
        case \/-(set) ⇒
          MongoStream(eval(Task now { client: MongoClient ⇒
            Task {
              val logger = Logger.getLogger("query")
              scalaz.stream.io.resource(
                Task delay {
                  val collection = client.getDB(set.db).getCollection(set.collName)
                  val c = collection.find(set.q)
                  set.sortQuery.foreach(c.sort(_))
                  set.skip.foreach(c.skip(_))
                  set.limit.foreach(c.limit(_))
                  set.maxTimeMS.foreach(c.maxTime(_, TimeUnit.MILLISECONDS))
                  logger.debug(s"Cursor: ${c.##} Query: ${set.q} Sort: ${set.sortQuery}")
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
