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

package mongo.join

import mongo.dsl3.Query
import org.apache.log4j.Logger

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.util.Try
import rx.lang.scala.schedulers.ExecutionContextScheduler
import rx.lang.scala.{ Observable, Subscriber }
import rx.lang.scala.Producer

package object observable {
  import Query._

  trait ObservableS extends DBTypes {
    override type DBStream[Out] = Observable[Out]
  }

  object ObservableS {
    trait Fetcher[T] {
      def db: String
      def coll: String
      def q: ObservableS#DBRecord
      def c: ObservableS#Client
      def log: Logger
      def subscriber: Subscriber[T]

      lazy val cursor: Option[ObservableS#Cursor] = (Try {
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

      def extract(c: ObservableS#Cursor): T = {
        val r = c.next.asInstanceOf[T]
        log.info(s"fetch $r")
        r
      }

      def fetch(n: Long): Unit = go(n)
    }

    class QueryProducer[T](val subscriber: Subscriber[T], val db: String, val coll: String,
                           val q: ObservableS#DBRecord, val c: ObservableS#Client, val log: Logger) extends Producer {
      self: { def fetch(n: Long) } ⇒
      override def request(n: Long) = fetch(n)
    }

    trait PKFetcher[T] extends Fetcher[T] {
      def key: String
      abstract override def extract(c: ObservableS#Cursor): T = {
        super.extract(c).asInstanceOf[ObservableS#DBRecord].get(key).asInstanceOf[T]
      }
    }

    implicit object joiner extends Joiner[ObservableS] {
      //for short
      type Record = ObservableS#DBRecord

      val scheduler = ExecutionContextScheduler(ExecutionContext.fromExecutor(exec))

      private def resource[A](q: Record, db: String, coll: String): Observable[A] = {
        log.info(s"[$db - $coll] Query: $q")
        Observable { subscriber: Subscriber[A] ⇒
          subscriber.setProducer(new QueryProducer[A](subscriber, db, coll, q, client, log) with Fetcher[A])
        }.subscribeOn(scheduler)
      }

      private def typedResource[A](q: Record, db: String, coll: String, keyField: String): Observable[A] = {
        log.info(s"[$db - $coll] Query: $q")
        Observable { subscriber: Subscriber[A] ⇒
          subscriber.setProducer(new QueryProducer[A](subscriber, db, coll, q, client, log) with PKFetcher[A] {
            override val key = keyField
          })
        }.subscribeOn(scheduler)
      }

      /**
       *
       * @param q
       * @param db
       * @param coll
       * @param key
       * @tparam A
       * @return
       */
      override def leftField[A](q: QueryFree[Record], db: String, coll: String, key: String): ObservableS#DBStream[A] =
        typedResource[A](createQuery(q), db, coll, key)

      /**
       *
       * @param q
       * @param db
       * @param coll
       * @return
       */
      override def left(q: QueryFree[Record], db: String, coll: String): Observable[ObservableS#DBRecord] =
        resource[Record](createQuery(q), db, coll)

      /**
       *
       * @param r
       * @param db
       * @param coll
       * @tparam A
       * @tparam B
       * @return
       */
      override def relationField[A, B](r: (A) ⇒ QueryFree[Record], db: String, coll: String): (A) ⇒ ObservableS#DBStream[B] =
        id ⇒
          resource[B](createQuery(r(id)), db, coll)

      /**
       *
       * @param r
       * @param db
       * @param coll
       * @return
       */
      override def relation(r: (Record) ⇒ QueryFree[Record], db: String, coll: String): (Record) ⇒ Observable[Record] =
        id ⇒
          resource[Record](createQuery(r(id)), db, coll)

      override def innerJoin[A, B, C](l: Observable[A])(relation: (A) ⇒ Observable[B])(f: (A, B) ⇒ C): ObservableS#DBStream[C] =
        for {
          id ← l
          rs ← relation(id).map(f(id, _))
        } yield rs
    }
  }

  implicit class ObservableSyntax[T](val self: Observable[T]) extends AnyVal {
    def column[B](name: String): Observable[B] =
      self.map { record ⇒
        record match {
          case r: ObservableS#DBRecord ⇒ r.get(name).asInstanceOf[B]
          case other                   ⇒ throw new com.mongodb.MongoException(s"DBObject expected but found ${other.getClass.getName}")
        }
      }

    def innerJoin[E, C](f: T ⇒ Observable[E])(m: (T, E) ⇒ C): Observable[C] =
      self.flatMap { id ⇒ f(id).map(m(id, _)) }

    def innerJoinRaw[E](f: T ⇒ Observable[T])(m: (T, T) ⇒ E): Observable[E] =
      self.flatMap { id ⇒ f(id).map(m(id, _)) }
  }
}