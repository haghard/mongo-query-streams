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

import java.text.MessageFormat
import java.util.concurrent.ExecutorService

import dsl.QFree
import dsl.cassandra.CassandraQueryInterpreter
import dsl.mongo.MongoQueryInterpreter
import join.StorageModule
import mongo.query.DBChannel
import org.apache.log4j.Logger
import com.mongodb.{ DBObject, MongoClient }
import com.datastax.driver.core.{ Session, Row, Cluster }
import rx.lang.scala.schedulers.ExecutionContextScheduler
import rx.lang.scala.{ Producer, Subscriber, Observable }

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.util.Try
import scalaz.concurrent.Task
import scalaz.stream.{ Cause, io }
import scalaz.stream.Process

package object storage {

  private def scheduler(exec: ExecutorService) =
    ExecutionContextScheduler(ExecutionContext.fromExecutor(exec))

  private trait Fetcher[T <: StorageModule, E] {
    def resource: String
    def collection: String
    def c: T#Client
    def q: T#ReadSettings
    def log: Logger
    def subscriber: Subscriber[E]
    def cursor: Option[T#Cursor]

    @tailrec private def go(n: Long): Unit = {
      log.info(s"${##} request $n")
      if (n > 0) {
        if (cursor.find(_.hasNext).isDefined) {
          subscriber.onNext(extract(cursor.get))
          go(n - 1)
        } else subscriber.onCompleted
      }
    }
    protected def extract(c: T#Cursor): E = {
      val r = c.next.asInstanceOf[E]
      log.info(s"fetch $r")
      r
    }

    def fetch(n: Long): Unit = go(n)
  }

  private class QueryProducer[T <: StorageModule, E](
      val subscriber: Subscriber[E], val resource: String,
      val collection: String, val q: T#ReadSettings, val c: T#Client, val log: Logger) extends Producer {
    self: Fetcher[T, E] ⇒
    override def request(n: Long) = fetch(n)
  }

  abstract class Storage[T <: StorageModule] {
    import join.mongo._
    import join.cassandra._

    private val initM = MongoReadSettings(new com.mongodb.BasicDBObject)
    private val initC = CassandraReadSettings("")

    def outerR(q: QFree[T#ReadSettings], collection: String, resource: String,
               log: Logger, exec: ExecutorService): T#Client ⇒ T#Stream[T#Record]

    def innerR(r: T#Record ⇒ QFree[T#ReadSettings], collection: String, resource: String,
               log: Logger, exec: ExecutorService): T#Client ⇒ (T#Record ⇒ T#Stream[T#Record])

    protected def mongoQuery(q: QFree[T#ReadSettings]) =
      scalaz.Free.runFC(q)(MongoQueryInterpreter).run(initM)._1

    protected def cassandraQuery(q: QFree[T#ReadSettings]) =
      scalaz.Free.runFC(q)(CassandraQueryInterpreter).run(initC)._1
  }

  object Storage {
    import join.mongo.{ MongoObservable, MongoProcess, MongoReadSettings }
    import join.cassandra.{ CassandraObservable, CassandraProcess, CassandraReadSettings }

    implicit object MongoStorageProcess extends Storage[MongoProcess] {

      private def mongo[T](qs: MongoReadSettings, client: MongoClient, c: String, resource: String,
                           logger: Logger, exec: ExecutorService): Process[Task, T] =
        io.resource(Task.delay {
          val coll = client.getDB(resource).getCollection(c)
          val cursor = coll.find(qs.q)
          qs.sort.foreach(cursor.sort(_))
          qs.skip.foreach(cursor.skip(_))
          qs.limit.foreach(cursor.limit(_))
          logger.debug(s"Query-settings: Sort:[ ${qs.sort} ] Skip:[ ${qs.skip} ] Limit:[ ${qs.limit} ] Query:[ ${qs.q} ]")
          cursor
        })(c ⇒ Task.delay(c.close)) { c ⇒
          Task {
            if (c.hasNext) {
              val r = c.next
              logger.debug(s"fetch $r")
              r.asInstanceOf[T]
            } else throw Cause.Terminated(Cause.End)
          }(exec)
        }

      override def outerR(qs: QFree[MongoProcess#ReadSettings], collection: String, resName: String,
                          logger: Logger, exec: ExecutorService): (MongoClient) ⇒ DBChannel[MongoClient, DBObject] =
        client ⇒
          DBChannel[MongoProcess#Client, MongoProcess#Record](Process.eval(Task.now { client: MongoProcess#Client ⇒
            Task(mongo[MongoProcess#Record](mongoQuery(qs), client, collection, resName, logger, exec))
          }))

      override def innerR(r: (DBObject) ⇒ QFree[MongoReadSettings], c: String, resource: String,
                          log: Logger, exec: ExecutorService): (MongoClient) ⇒ (DBObject) ⇒ DBChannel[MongoClient, DBObject] = {
        client ⇒
          parent ⇒
            DBChannel[MongoProcess#Client, MongoProcess#Record](Process.eval(Task.now { client: MongoProcess#Client ⇒
              Task(mongo[MongoProcess#Record](mongoQuery(r(parent)), client, c, resource, log, exec))
            }))
      }
    }

    implicit object MongoStorageObservable extends Storage[MongoObservable] {
      private trait MongoFetcher[E] extends Fetcher[MongoObservable, E] {
        lazy val cursor: Option[MongoObservable#Cursor] = (Try {
          Option {
            val coll = c.getDB(resource).getCollection(collection)
            val cursor = coll.find(q.q)
            q.sort.foreach(cursor.sort(_))
            q.skip.foreach(cursor.skip(_))
            q.limit.foreach(cursor.limit(_))
            log.debug(s"Query-settings: Sort:[ ${q.sort} ] Skip:[ ${q.skip} ] Limit:[ ${q.limit} ] Query:[ ${q.q} ]")
            cursor
          }
        } recover {
          case e: Throwable ⇒
            subscriber.onError(e)
            None
        }).get
      }

      private def mongo[A](q: MongoObservable#ReadSettings, c: String, resource: String,
                           log: Logger, client: MongoObservable#Client, exec: ExecutorService): Observable[A] = {
        Observable { subscriber: Subscriber[A] ⇒
          subscriber.setProducer(new QueryProducer[MongoObservable, A](subscriber, resource, c, q, client, log) with MongoFetcher[A])
        }.subscribeOn(scheduler(exec))
      }

      override def outerR(q: QFree[MongoObservable#ReadSettings], c: String, resource: String,
                          log: Logger, exec: ExecutorService): (MongoClient) ⇒ Observable[DBObject] = {
        client ⇒
          mongo[MongoObservable#Record](mongoQuery(q), c, resource, log, client, exec)
      }

      override def innerR(r: (DBObject) ⇒ QFree[MongoReadSettings], c: String, resource: String,
                          log: Logger, exec: ExecutorService): (MongoClient) ⇒ (DBObject) ⇒ Observable[DBObject] =
        client ⇒
          parent ⇒
            mongo[MongoObservable#Record](mongoQuery(r(parent)), c, resource, log, client, exec)
    }

    implicit object CassandraStorageProcess extends Storage[CassandraProcess] {
      private[this] def cassandra[T](qs: CassandraReadSettings, session: Session, c: String,
                                     logger: Logger, exec: ExecutorService): Process[Task, T] =
        io.resource(Task.delay {
          val query = MessageFormat.format(qs.q, c)
          logger.debug(s"Query-settings: Query:[ $query ] Param: [ ${qs.v} ]")
          qs.v.fold(session.execute(session.prepare(query).setConsistencyLevel(qs.consistencyLevel).bind()).iterator) { r ⇒
            session.execute(session.prepare(query).setConsistencyLevel(qs.consistencyLevel).bind(r.v)).iterator
          }
        })(c ⇒ Task.delay(session.close)) { c ⇒
          Task {
            if (c.hasNext) {
              val r = c.next
              logger.debug(s"fetch $r")
              r.asInstanceOf[T]
            } else throw Cause.Terminated(Cause.End)
          }(exec)
        }

      override def outerR(q: QFree[CassandraReadSettings], c: String, r: String,
                          log: Logger, exec: ExecutorService): (Cluster) ⇒ DBChannel[Cluster, Row] = {
        client ⇒
          DBChannel[CassandraProcess#Client, CassandraProcess#Record](Process.eval(Task.now { client: CassandraProcess#Client ⇒
            Task(cassandra[CassandraProcess#Record](cassandraQuery(q), client.connect(r), c, log, exec))
          }))
      }

      override def innerR(r: (Row) ⇒ QFree[CassandraReadSettings], c: String, res: String,
                          log: Logger, exec: ExecutorService): (Cluster) ⇒ (Row) ⇒ DBChannel[Cluster, Row] = {
        client ⇒
          parent ⇒
            DBChannel[CassandraProcess#Client, CassandraProcess#Record](Process.eval(Task.now { client: CassandraProcess#Client ⇒
              Task(cassandra[CassandraProcess#Record](cassandraQuery(r(parent)), client.connect(res), c, log, exec))
            }))
      }
    }

    implicit object CassandraStorageObservable extends Storage[CassandraObservable] {
      private trait CassandraFetcher[E] extends Fetcher[CassandraObservable, E] {
        lazy val cursor: Option[CassandraObservable#Cursor] = (Try {
          Option {
            val query = MessageFormat.format(q.q, collection)
            val session = c.connect(resource)
            log.debug(s"Query-settings: Query:[ $query ] Param: [ ${q.v} ]")
            q.v.fold(session.execute(session.prepare(query).setConsistencyLevel(q.consistencyLevel).bind()).iterator()) { r ⇒
              session.execute(session.prepare(query).setConsistencyLevel(q.consistencyLevel).bind(r.v)).iterator()
            }
          }
        } recover {
          case e: Throwable ⇒
            subscriber.onError(e)
            None
        }).get
      }

      private def cassandra[A](q: CassandraObservable#ReadSettings, client: CassandraObservable#Client,
                               c: String, resource: String, log: Logger, exec: ExecutorService): Observable[A] = {
        Observable { subscriber: Subscriber[A] ⇒
          subscriber.setProducer(new QueryProducer[CassandraObservable, A](subscriber, resource, c, q, client, log) with CassandraFetcher[A])
        }.subscribeOn(scheduler(exec))
      }

      override def outerR(q: QFree[CassandraReadSettings], c: String, resource: String,
                          log: Logger, exec: ExecutorService): (Cluster) ⇒ Observable[Row] = {
        client ⇒ cassandra[CassandraObservable#Record](cassandraQuery(q), client, c, resource, log, exec)
      }

      override def innerR(r: (Row) ⇒ QFree[CassandraReadSettings], c: String, res: String,
                          log: Logger, exec: ExecutorService): (Cluster) ⇒ (Row) ⇒ Observable[Row] = {
        client ⇒
          parent ⇒ cassandra[CassandraObservable#Record](cassandraQuery(r(parent)), client, c, res, log, exec)
      }
    }

    /**
     *
     * @tparam T
     * @return
     */
    def apply[T <: StorageModule: Storage]: Storage[T] = implicitly[Storage[T]]
  }
}
