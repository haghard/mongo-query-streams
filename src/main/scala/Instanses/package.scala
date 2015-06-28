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

import join.DBModule
import scala.util.Try
import java.text.MessageFormat
import join.process.MongoProcessStream
import java.util.concurrent.ExecutorService
import join.observable.MongoObservableStream
import mongo.dsl.{ qb, MongoQuerySettings }
import mongo.dsl.qb._
import mongo.query.DBChannel
import org.apache.log4j.Logger
import scala.annotation.tailrec
import scalaz.concurrent.Task
import scala.concurrent.ExecutionContext
import scalaz.stream.{ Cause, io, Process }
import rx.lang.scala.schedulers.ExecutionContextScheduler
import rx.lang.scala.{ Producer, Subscriber, Observable }
import cassandra.{ CassandraObservable, CassandraQuerySettings, CassandraProcessStream }

package object joiners {

  trait JoinerG[T <: DBModule] {
    protected var log: Logger = _
    protected var client: T#Client = _
    protected var exec: ExecutorService = _

    private val initM = MongoQuerySettings(new com.mongodb.BasicDBObject)
    private val initC = CassandraQuerySettings("")

    private def withClient(client: T#Client) = {
      this.client = client
      this
    }

    private def withLogger(log: Logger) = {
      this.log = log
      this
    }

    private def withExecutor(ex: ExecutorService) = {
      exec = ex
      this
    }

    protected def createMQuery(q: QueryFree[T#QuerySettings]) =
      scalaz.Free.runFC[StatementOp, QueryM, T#QuerySettings](q)(qb.MongoQueryInterpreter).run(initM)._1

    protected def createCQuery(q: QueryFree[T#QuerySettings]): CassandraQuerySettings =
      scalaz.Free.runFC[StatementOp, QueryC, T#QuerySettings](q)(qb.CassandraQueryInterpreter).run(initC)._1

    def left(q: QueryFree[T#QuerySettings], collection: String, db: String): T#DBStream[T#DBRecord]
    def right(q: T#DBRecord ⇒ QueryFree[T#QuerySettings], collection: String, db: String): (T#DBRecord ⇒ T#DBStream[T#DBRecord])
    def join[A, B, C](l: T#DBStream[A])(relation: A ⇒ T#DBStream[B])(f: (A, B) ⇒ C): T#DBStream[C]
  }

  private[joiners] def mongoResource[T](qs: MongoProcessStream#QuerySettings, client: MongoProcessStream#Client, db: String,
                                        collection: String, logger: Logger, exec: ExecutorService): Process[Task, T] = {
    io.resource(Task.delay {
      val coll = client.getDB(db).getCollection(collection)
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
  }

  private[joiners] def cassandraResource[T](q: CassandraProcessStream#QuerySettings, client: CassandraProcessStream#Client, keySpace: String,
                                            table: String, logger: Logger, exec: ExecutorService): Process[Task, T] = {
    io.resource(Task.delay {
      val readConsistency: com.datastax.driver.core.ConsistencyLevel = com.datastax.driver.core.ConsistencyLevel.ONE
      val queryWithTable = MessageFormat.format(q.q, table)
      val session = client.connect(keySpace)
      logger.debug(s"Query-settings: Query:[ $queryWithTable ] Param: [ ${q.v} ]")
      q.v.fold(session.execute(session.prepare(queryWithTable).setConsistencyLevel(readConsistency).bind()).iterator) { r ⇒
        session.execute(session.prepare(queryWithTable).setConsistencyLevel(readConsistency).bind(r.v)).iterator
      }
    })(c ⇒ Task.delay(())) { c ⇒
      Task {
        if (c.hasNext) {
          val r = c.next
          logger.debug(s"fetch row $r")
          r.asInstanceOf[T]
        } else throw Cause.Terminated(Cause.End)
      }(exec)
    }
  }

  object JoinerG {
    trait Fetcher[T] {
      def db: String
      def coll: String
      def q: MongoObservableStream#DBRecord
      def c: MongoObservableStream#Client
      def log: Logger
      def subscriber: Subscriber[T]

      lazy val cursor: Option[MongoObservableStream#Cursor] = (Try {
        Option(c.getDB(db).getCollection(coll).find(q))
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

    implicit object MongoP extends JoinerG[MongoProcessStream] {
      override def left(q: QueryFree[MongoProcessStream#QuerySettings], collection: String, db: String) =
        DBChannel[MongoProcessStream#Client, MongoProcessStream#DBRecord](Process.eval(Task.now { client: MongoProcessStream#Client ⇒
          Task(mongoResource[MongoProcessStream#DBRecord](createMQuery(q), client, db, collection, log, exec))
        }))

      override def right(r: MongoProcessStream#DBRecord ⇒ QueryFree[MongoProcessStream#QuerySettings], collection: String, db: String): MongoProcessStream#DBRecord ⇒ MongoProcessStream#DBStream[MongoProcessStream#DBRecord] = {
        parent ⇒
          DBChannel[MongoProcessStream#Client, MongoProcessStream#DBRecord](Process.eval(Task.now { client: MongoProcessStream#Client ⇒
            Task(mongoResource[MongoProcessStream#DBRecord](createMQuery(r(parent)), client, db, collection, log, exec))
          }))
      }
      def join[A, B, C](l: MongoProcessStream#DBStream[A])(relation: A ⇒ MongoProcessStream#DBStream[B])(f: (A, B) ⇒ C): MongoProcessStream#DBStream[C] =
        for { id ← l; rs ← relation(id) |> scalaz.stream.process1.lift(f(id, _)) } yield rs
    }

    class QueryProducer[T](val subscriber: Subscriber[T], val db: String, val coll: String,
                           val q: MongoObservableStream#DBRecord, val c: MongoObservableStream#Client, val log: Logger) extends Producer {
      self: { def fetch(n: Long) } ⇒
      override def request(n: Long) = fetch(n)
    }

    private[joiners] def scheduler(exec: ExecutorService) = ExecutionContextScheduler(ExecutionContext.fromExecutor(exec))
    private[joiners] def resource[A](q: MongoObservableStream#QuerySettings, db: String, coll: String,
                                     log: Logger, client: MongoObservableStream#Client, scheduler: ExecutionContextScheduler): Observable[A] = {
      log.info(s"[$db - $coll] Query: $q")
      Observable { subscriber: Subscriber[A] ⇒
        subscriber.setProducer(new QueryProducer[A](subscriber, db, coll, q.q, client, log) with Fetcher[A])
      }.subscribeOn(scheduler)
    }

    implicit object MongoO extends JoinerG[MongoObservableStream] {
      override def left(q: QueryFree[MongoObservableStream#QuerySettings], collection: String, db: String) =
        resource[MongoObservableStream#DBRecord](createMQuery(q), db, collection, log, client, scheduler(exec))

      override def right(r: MongoObservableStream#DBRecord ⇒ QueryFree[MongoQuerySettings], collection: String, db: String) = {
        id ⇒
          resource[MongoObservableStream#DBRecord](createMQuery(r(id)), db, collection, log, client, scheduler(exec))
      }

      override def join[A, B, C](l: MongoObservableStream#DBStream[A])(relation: A ⇒ MongoObservableStream#DBStream[B])(f: (A, B) ⇒ C): MongoObservableStream#DBStream[C] =
        for {
          id ← l
          rs ← relation(id).map(f(id, _))
        } yield rs
    }

    implicit object CassandraP extends JoinerG[CassandraProcessStream] {
      override def left(q: QueryFree[CassandraProcessStream#QuerySettings], table: String, keySpace: String) = {
        DBChannel[CassandraProcessStream#Client, CassandraProcessStream#DBRecord](Process.eval(Task.now { client: CassandraProcessStream#Client ⇒
          Task(cassandraResource[CassandraProcessStream#DBRecord](createCQuery(q), client, keySpace, table, log, exec))
        }))
      }

      override def right(q: CassandraProcessStream#DBRecord ⇒ QueryFree[CassandraQuerySettings], table: String, keySpace: String) =
        parent ⇒
          DBChannel[CassandraProcessStream#Client, CassandraProcessStream#DBRecord](Process.eval(Task.now { client: CassandraProcessStream#Client ⇒
            Task(cassandraResource[CassandraProcessStream#DBRecord](createCQuery(q(parent)), client, keySpace, table, log, exec))
          }))

      override def join[A, B, C](l: CassandraProcessStream#DBStream[A])(relation: A ⇒ CassandraProcessStream#DBStream[B])(f: (A, B) ⇒ C): CassandraProcessStream#DBStream[C] =
        for { id ← l; rs ← relation(id) |> scalaz.stream.process1.lift(f(id, _)) } yield rs
    }

    implicit object CassandraO extends JoinerG[CassandraObservable] {
      override def left(q: QueryFree[CassandraObservable#QuerySettings], collection: String, db: String) = ???
      override def right(q: CassandraObservable#DBRecord ⇒ QueryFree[CassandraQuerySettings], collection: String, db: String) = ???
      override def join[A, B, C](l: Observable[A])(relation: (A) ⇒ Observable[B])(f: (A, B) ⇒ C): Observable[C] = ???
    }

    def apply[T <: DBModule](implicit j: JoinerG[T], c: T#Client, log: Logger, pool: ExecutorService): JoinerG[T] =
      j.withExecutor(pool).withLogger(log).withClient(c)
  }
}
