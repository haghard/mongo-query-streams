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

import join.observable.MongoObservableStream
import joinG.JoinerG
import join.DBModule
import mongo.dsl.qb._
import mongo.query.DBChannel
import org.apache.log4j.Logger
import com.mongodb.{ DBObject, MongoClient }
import mongo.dsl.{ qb, MongoQuerySettings }
import join.process.MongoProcessStream
import com.datastax.driver.core.{ Session, Row, Cluster }
import cassandra.{ CassandraObservableStream, CassandraQuerySettings, CassandraProcessStream }
import rx.lang.scala.schedulers.ExecutionContextScheduler
import rx.lang.scala.{ Producer, Subscriber, Observable }

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.util.Try
import scalaz.concurrent.Task
import scalaz.stream.{ Cause, io }
import scalaz.stream.Process

object storage {

  abstract class Storage[T <: DBModule: JoinerG] {
    private val initM = MongoQuerySettings(new com.mongodb.BasicDBObject)
    private val initC = CassandraQuerySettings("")

    def resource(q: QueryFree[T#QuerySettings], collection: String, resource: String, log: Logger, exec: ExecutorService): T#Client ⇒ T#DBStream[T#DBRecord]

    def resource(r: T#DBRecord ⇒ QueryFree[T#QuerySettings], collection: String, resource: String, log: Logger, exec: ExecutorService): T#Client ⇒ (T#DBRecord ⇒ T#DBStream[T#DBRecord])

    protected def cassandra[T](qs: CassandraQuerySettings, session: Session, collection: String, logger: Logger, exec: ExecutorService) =
      io.resource(Task.delay {
        val readConsistency: com.datastax.driver.core.ConsistencyLevel = com.datastax.driver.core.ConsistencyLevel.ONE
        val queryWithTable = MessageFormat.format(qs.q, collection)
        logger.debug(s"Query-settings: Query:[ $queryWithTable ] Param: [ ${qs.v} ]")
        qs.v.fold(session.execute(session.prepare(queryWithTable).setConsistencyLevel(readConsistency).bind()).iterator) { r ⇒
          session.execute(session.prepare(queryWithTable).setConsistencyLevel(readConsistency).bind(r.v)).iterator
        }
      })(c ⇒ Task.delay(session.close)) { c ⇒
        Task {
          if (c.hasNext) {
            val r = c.next
            logger.debug(s"fetch $r")
            r.asInstanceOf[CassandraProcessStream#DBRecord]
          } else throw Cause.Terminated(Cause.End)
        }(exec)
      }

    protected def mongo[T](qs: MongoQuerySettings, client: MongoClient, collection: String, resource: String, logger: Logger, exec: ExecutorService) =
      io.resource(Task.delay {
        val coll = client.getDB(resource).getCollection(collection)
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

    protected def mongo2[A](q: MongoObservableStream#QuerySettings, coll: String, resource: String,
                            log: Logger, client: MongoObservableStream#Client, exec: ExecutorService): Observable[A] = {
      log.info(s"[$resource - $coll] Query: $q")
      Observable { subscriber: Subscriber[A] ⇒
        subscriber.setProducer(new MongoQueryProducer[A](subscriber, resource, coll, q, client, log) with MongoFetcher[A])
      }.subscribeOn(scheduler(exec))
    }

    protected def cassandra2[A](q: CassandraObservableStream#QuerySettings, client: CassandraObservableStream#Client,
                                collection: String, resource: String,
                                log: Logger, exec: ExecutorService): Observable[A] = {
      log.info(s"[$resource - $collection] Query: $q")
      Observable { subscriber: Subscriber[A] ⇒
        subscriber.setProducer(new CassandraQueryProducer[A](subscriber, resource, collection, q, client, log) with CassandraFetcher[A])
      }.subscribeOn(scheduler(exec))
    }

    protected def createMQuery(q: QueryFree[T#QuerySettings]): MongoQuerySettings =
      scalaz.Free.runFC[StatementOp, QueryM, T#QuerySettings](q)(qb.MongoQueryInterpreter).run(initM)._1

    protected def createCQuery(q: QueryFree[T#QuerySettings]): CassandraQuerySettings =
      scalaz.Free.runFC[StatementOp, QueryC, T#QuerySettings](q)(qb.CassandraQueryInterpreter).run(initC)._1

    private[this] def scheduler(exec: ExecutorService) = ExecutionContextScheduler(ExecutionContext.fromExecutor(exec))

    private[this] trait CassandraFetcher[T] {
      def resource: String
      def collection: String
      def c: CassandraObservableStream#Client
      def q: CassandraObservableStream#QuerySettings
      def log: Logger
      def subscriber: Subscriber[T]

      lazy val cursor: Option[CassandraObservableStream#Cursor] = (Try {
        Option {
          val readConsistency: com.datastax.driver.core.ConsistencyLevel = com.datastax.driver.core.ConsistencyLevel.ONE
          val queryWithTable = MessageFormat.format(q.q, collection)
          val session = c.connect(resource)
          log.debug(s"Query-settings: Query:[ $queryWithTable ] Param: [ ${q.v} ]")
          q.v.fold(session.execute(session.prepare(queryWithTable).setConsistencyLevel(readConsistency).bind()).iterator()) { r ⇒
            session.execute(session.prepare(queryWithTable).setConsistencyLevel(readConsistency).bind(r.v)).iterator()
          }
        }
      } recover {
        case e: Throwable ⇒
          subscriber.onError(e)
          None
      }).get

      @tailrec private def go(n: Long): Unit = {
        log.info(s"${##} request $n")
        if (n > 0) {
          if (cursor.find(_.hasNext).isDefined) {
            subscriber.onNext(extract(cursor.get))
            go(n - 1)
          } else subscriber.onCompleted
        }
      }
      def extract(c: CassandraObservableStream#Cursor): T = {
        val r = c.next.asInstanceOf[T]
        log.info(s"fetch $r")
        r
      }

      def fetch(n: Long): Unit = go(n)
    }

    private[this] trait MongoFetcher[T] {
      def resource: String
      def collection: String
      def c: MongoObservableStream#Client
      def q: MongoObservableStream#QuerySettings
      def log: Logger
      def subscriber: Subscriber[T]

      lazy val cursor: Option[MongoObservableStream#Cursor] = (Try {
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

      @tailrec private def go(n: Long): Unit = {
        log.info(s"${##} request $n")
        if (n > 0) {
          if (cursor.find(_.hasNext).isDefined) {
            subscriber.onNext(extract(cursor.get))
            go(n - 1)
          } else subscriber.onCompleted
        }
      }
      def extract(c: MongoObservableStream#Cursor): T = {
        val r = c.next.asInstanceOf[T]
        log.info(s"fetch $r")
        r
      }
      def fetch(n: Long): Unit = go(n)
    }

    private[this] class MongoQueryProducer[T](val subscriber: Subscriber[T], val resource: String,
                                              val collection: String, val q: MongoObservableStream#QuerySettings,
                                              val c: MongoObservableStream#Client, val log: Logger) extends Producer {
      self: MongoFetcher[T] ⇒
      override def request(n: Long) = fetch(n)
    }

    private[this] class CassandraQueryProducer[T](val subscriber: Subscriber[T], val resource: String,
                                                  val collection: String, val q: CassandraObservableStream#QuerySettings,
                                                  val c: CassandraObservableStream#Client, val log: Logger) extends Producer {
      self: CassandraFetcher[T] ⇒
      override def request(n: Long) = fetch(n)
    }
  }

  object Storage {
    def apply[T <: DBModule: Storage]: Storage[T] = implicitly[Storage[T]]

    implicit object MongoStorage extends Storage[MongoProcessStream] {
      override def resource(qs: QueryFree[MongoProcessStream#QuerySettings], collection: String, resName: String,
                            logger: Logger, exec: ExecutorService): (MongoClient) ⇒ DBChannel[MongoClient, DBObject] =
        client ⇒
          DBChannel[MongoProcessStream#Client, MongoProcessStream#DBRecord](Process.eval(Task.now { client: MongoProcessStream#Client ⇒
            Task(mongo[MongoProcessStream#DBRecord](createMQuery(qs), client, collection, resName, logger, exec))
          }))

      override def resource(r: (DBObject) ⇒ QueryFree[MongoQuerySettings], collection: String, resName: String, log: Logger, exec: ExecutorService): (MongoClient) ⇒ (DBObject) ⇒ DBChannel[MongoClient, DBObject] = {
        client ⇒
          parent ⇒
            DBChannel[MongoProcessStream#Client, MongoProcessStream#DBRecord](Process.eval(Task.now { client: MongoProcessStream#Client ⇒
              Task(mongo[MongoProcessStream#DBRecord](createMQuery(r(parent)), client, collection, resName, log, exec))
            }))
      }
    }

    implicit object MongoStorage2 extends Storage[MongoObservableStream] {
      override def resource(q: QueryFree[MongoObservableStream#QuerySettings], collection: String, resName: String,
                            log: Logger, exec: ExecutorService): (MongoClient) ⇒ Observable[DBObject] = {
        client ⇒
          mongo2[MongoObservableStream#DBRecord](createMQuery(q), collection, resName, log, client, exec)
      }

      override def resource(r: (DBObject) ⇒ QueryFree[MongoQuerySettings], collection: String, resName: String, log: Logger, exec: ExecutorService): (MongoClient) ⇒ (DBObject) ⇒ Observable[DBObject] =
        client ⇒
          parent ⇒
            mongo2[MongoObservableStream#DBRecord](createMQuery(r(parent)), collection, resName, log, client, exec)
    }

    implicit object CassandraStorage extends Storage[CassandraProcessStream] {
      override def resource(q: QueryFree[CassandraQuerySettings], collection: String, resource: String, log: Logger, exec: ExecutorService): (Cluster) ⇒ DBChannel[Cluster, Row] =
        client ⇒
          DBChannel[CassandraProcessStream#Client, CassandraProcessStream#DBRecord](Process.eval(Task.now { client: CassandraProcessStream#Client ⇒
            Task(cassandra[CassandraProcessStream#DBRecord](createCQuery(q), client.connect(resource), collection, log, exec))
          }))

      override def resource(r: (Row) ⇒ QueryFree[CassandraQuerySettings], collection: String, resource: String, log: Logger, exec: ExecutorService): (Cluster) ⇒ (Row) ⇒ DBChannel[Cluster, Row] =
        client ⇒
          parent ⇒
            DBChannel[CassandraProcessStream#Client, CassandraProcessStream#DBRecord](Process.eval(Task.now { client: CassandraProcessStream#Client ⇒
              Task(cassandra[CassandraProcessStream#DBRecord](createCQuery(r(parent)), client.connect(resource), collection, log, exec))
            }))
    }

    implicit object CassandraStorage2 extends Storage[CassandraObservableStream] {
      override def resource(q: QueryFree[CassandraQuerySettings], collection: String, resource: String, log: Logger, exec: ExecutorService): (Cluster) ⇒ Observable[Row] =
        client ⇒
          cassandra2[CassandraObservableStream#DBRecord](createCQuery(q), client, collection, resource, log, exec)

      override def resource(r: (Row) ⇒ QueryFree[CassandraQuerySettings], collection: String, resource: String, log: Logger, exec: ExecutorService): (Cluster) ⇒ (Row) ⇒ Observable[Row] =
        client ⇒
          parent ⇒
            cassandra2[CassandraObservableStream#DBRecord](createCQuery(r(parent)), client, collection, resource, log, exec)

    }
  }
}
