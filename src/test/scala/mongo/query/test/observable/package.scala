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

package mongo.query.test

import java.util.concurrent.ExecutorService
import mongo.dsl3.Interaction.Streamer
import mongo.dsl3.Query
import org.apache.log4j.Logger
import rx.lang.scala.schedulers.ExecutionContextScheduler
import rx.lang.scala.{ Producer, Observable, Subscriber }
import com.mongodb._

import scala.concurrent.ExecutionContext
import scala.util.Try
import scala.annotation.tailrec
import mongo._

package object observable {

  implicit val M = new scalaz.Monad[Observable]() {
    override def point[A](a: ⇒ A): Observable[A] = Observable.just(a)
    override def bind[A, B](fa: Observable[A])(f: (A) ⇒ Observable[B]): Observable[B] = fa.flatMap(f)
  }

  implicit object RxStreamer extends Streamer[Observable] {
    override def create[T](q: BasicDBObject, client: MongoClient, db: String, coll: String)(implicit pool: ExecutorService): Observable[T] = {
      Observable { subscriber: Subscriber[T] ⇒
        subscriber.setProducer(new Producer() {
          lazy val cursor: Option[DBCursor] = (Try {
            Option(client.getDB(db).getCollection(coll).find(q))
          } recover {
            case e: Throwable ⇒
              subscriber.onError(e)
              None
          }).get

          @tailrec def go(n: Long): Unit = {
            if (n > 0) {
              if (cursor.find(_.hasNext).isDefined) {
                val r = cursor.get.next().asInstanceOf[T]
                logger.info(s"fetch $r")
                subscriber.onNext(r)
                go(n - 1)
              } else subscriber.onCompleted
            }
          }

          override def request(n: Long): Unit = {
            logger.info(s"${##} request $n")
            go(n)
          }
        })
      }.subscribeOn(ExecutionContextScheduler(ExecutionContext.fromExecutor(pool)))
    }
  }

  trait MongoObservableT extends join.STypes {
    type MStream[Out] = Observable[Out]
  }

  object MongoObservableT {
    import scalaz.Free.runFC
    import Query._

    trait Fetcher[T] {
      def db: String
      def coll: String
      def q: BasicDBObject
      def c: MongoClient
      def log: Logger
      def subscriber: Subscriber[T]

      lazy val cursor: Option[DBCursor] = (Try {
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

      def extract(c: DBCursor): T = {
        val r = c.next.asInstanceOf[T]
        log.info(s"fetch $r")
        r
      }

      def fetch(n: Long): Unit = go(n)
    }

    class QueryProducer[T](val subscriber: Subscriber[T], val db: String, val coll: String,
                           val q: BasicDBObject, val c: MongoClient, val log: Logger) extends Producer {
      self: { def fetch(n: Long) } ⇒
      override def request(n: Long) = fetch(n)
    }

    trait PKFetcher[T] extends Fetcher[T] {
      def key: String
      abstract override def extract(c: DBCursor): T = {
        super.extract(c).asInstanceOf[DBObject].get(key).asInstanceOf[T]
      }
    }

    implicit object joiner extends join.Joiner[MongoObservableT] {
      val init = new BasicDBObject

      def scheduler = ExecutionContextScheduler(ExecutionContext.fromExecutor(exec))

      private def resource[A](q: BasicDBObject, db: String, coll: String): Observable[A] = {
        Observable { subscriber: Subscriber[A] ⇒
          subscriber.setProducer(new QueryProducer[A](subscriber, db, coll, q, client, log) with Fetcher[A])
        }.subscribeOn(scheduler)
      }

      private def keyResource[A](q: BasicDBObject, db: String, coll: String, keyField: String): Observable[A] = {
        Observable { subscriber: Subscriber[A] ⇒
          subscriber.setProducer(new QueryProducer[A](subscriber, db, coll, q, client, log) with PKFetcher[A] { override val key = keyField })
        }.subscribeOn(scheduler)
      }

      override def left[A](q: Query.QueryBuilder[BasicDBObject], db: String, coll: String, key: String): MongoObservableT#MStream[A] = {
        val q0 = (runFC[StatementOp, QueryS, BasicDBObject](q)(Query.QueryInterpreterS)).run(init)._1
        log.info(s"[$db - $coll] query: $q0")
        keyResource[A](q0, db, coll, key)
      }

      override def relation[A, B](r: (A) ⇒ Query.QueryBuilder[BasicDBObject], db: String, coll: String): (A) ⇒ MongoObservableT#MStream[B] =
        id ⇒ {
          val q0 = (runFC[StatementOp, QueryS, BasicDBObject](r(id))(Query.QueryInterpreterS)).run(init)._1
          log.info(s"[$db - $coll] query: $q0")
          resource[B](q0, db, coll)
        }

      override def innerJoin[A, B, C](l: Observable[A])(relation: (A) ⇒ Observable[B])(f: (A, B) ⇒ C): MongoObservableT#MStream[C] =
        for {
          id ← l
          rs ← relation(id).map(f(id, _))
        } yield rs
      //l.flatMap { id ⇒ relation(id).map(f(id, _)) }
    }
  }

  implicit class ObservableSyntax[T](val self: Observable[T]) extends AnyVal {
    def column[B](name: String): Observable[B] =
      self.map { record ⇒
        record match {
          case r: DBObject ⇒ r.get(name).asInstanceOf[B]
          case other       ⇒ throw new MongoException(s"DBObject expected but found ${other.getClass.getName}")
        }
      }

    def innerJoin[E, C](f: T ⇒ Observable[E])(m: (T, E) ⇒ C): Observable[C] =
      self.flatMap { id ⇒ f(id).map(m(id, _)) }

    def innerJoinRaw[E](f: T ⇒ Observable[T])(m: (T, T) ⇒ E): Observable[E] =
      self.flatMap { id ⇒ f(id).map(m(id, _)) }
  }
}
