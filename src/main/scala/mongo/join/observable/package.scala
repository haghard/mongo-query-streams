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

import mongo.dsl.qb
import org.apache.log4j.Logger

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.util.Try
import rx.lang.scala.schedulers.ExecutionContextScheduler
import rx.lang.scala.{ Observable, Subscriber }
import rx.lang.scala.Producer

package object observable {
  import qb._

  trait MongoObservable extends DBModule {
    override type Client = com.mongodb.MongoClient
    override type DBRecord = com.mongodb.DBObject
    override type QuerySettings = mongo.dsl.QuerySettings
    override type Cursor = com.mongodb.Cursor
    override type DBStream[Out] = Observable[Out]
  }

  object MongoObservable {
    trait Fetcher[T] {
      def db: String
      def coll: String
      def q: MongoObservable#DBRecord
      def c: MongoObservable#Client
      def log: Logger
      def subscriber: Subscriber[T]

      lazy val cursor: Option[MongoObservable#Cursor] = (Try {
        Option(c.getDB(db).getCollection(coll).find(q))
      } recover {
        case e: Throwable ⇒
          subscriber.onError(e)
          None
      }).get

      @tailrec private def go(n: Long): Unit = {
        log.info(s"request $n")
        if (n > 0) {
          if (cursor.find(_.hasNext()).isDefined) {
            subscriber.onNext(extract(cursor.get))
            go(n - 1)
          } else subscriber.onCompleted()
        }
      }

      def extract(c: MongoObservable#Cursor): T = {
        val r = c.next.asInstanceOf[T]
        log.info(s"fetch $r")
        r
      }

      def fetch(n: Long): Unit = go(n)
    }

    class QueryProducer[T](val subscriber: Subscriber[T], val db: String, val coll: String,
                           val q: MongoObservable#DBRecord, val c: MongoObservable#Client, val log: Logger) extends Producer {
      self: Fetcher[T] ⇒
      override def request(n: Long) = fetch(n)
    }

    trait PKFetcher[T] extends Fetcher[T] {
      def key: String
      abstract override def extract(c: MongoObservable#Cursor): T =
        super.extract(c).asInstanceOf[MongoObservable#DBRecord].get(key).asInstanceOf[T]
    }

    implicit object joiner extends Joiner[MongoObservable] {
      val scheduler = ExecutionContextScheduler(ExecutionContext.fromExecutor(exec))

      private def resource[A](q: MongoObservable#QuerySettings, db: String, collection: String): Observable[A] = {
        log.info(s"[$db - $collection] Query: $q")
        Observable { subscriber: Subscriber[A] ⇒
          subscriber.setProducer(new QueryProducer[A](subscriber, db, collection, q.q, client, log) with Fetcher[A])
        }.subscribeOn(scheduler)
      }

      private def typedResource[A](q: MongoObservable#QuerySettings, db: String, collection: String, keyField: String): Observable[A] = {
        log.info(s"[$db - $collection] Query: $q")
        Observable { subscriber: Subscriber[A] ⇒
          subscriber.setProducer(new QueryProducer[A](subscriber, db, collection, q.q, client, log) with PKFetcher[A] {
            override val key = keyField
          })
        }.subscribeOn(scheduler)
      }

      override def leftField[A](query: QueryFree[MongoObservable#QuerySettings],
                                db: String, collection: String, key: String): MongoObservable#DBStream[A] =
        typedResource[A](createQuery(query), db, collection, key)

      override def left(query: QueryFree[MongoObservable#QuerySettings],
                        db: String, collection: String): Observable[MongoObservable#DBRecord] =
        resource[MongoObservable#DBRecord](createQuery(query), db, collection)

      /**
       *
       * @param relation
       * @param db
       * @param collection
       * @tparam A
       * @tparam B
       * @return (A ⇒ MongoObservableStream#DBStream[B])
       */
      override def relationField[A, B](relation: (A) ⇒ QueryFree[MongoObservable#QuerySettings],
                                       db: String, collection: String): (A) ⇒ MongoObservable#DBStream[B] =
        id ⇒
          resource[B](createQuery(relation(id)), db, collection)

      /**
       *
       * @param r
       * @param db
       * @param collection
       * @return
       */
      override def relation(r: (MongoObservable#DBRecord) ⇒ QueryFree[MongoObservable#QuerySettings],
                            db: String, collection: String): (MongoObservable#DBRecord) ⇒ Observable[MongoObservable#DBRecord] =
        id ⇒
          resource[MongoObservable#DBRecord](createQuery(r(id)), db, collection)

      override def innerJoin[A, B, C](left: Observable[A])(relation: (A) ⇒ Observable[B])(f: (A, B) ⇒ C): MongoObservable#DBStream[C] =
        for {
          id ← left
          rs ← relation(id).map(f(id, _))
        } yield rs
    }
  }

  implicit class ObservableSyntax[T](val self: Observable[T]) extends AnyVal {
    def column[B](name: String): Observable[B] =
      self map {
        case r: MongoObservable#DBRecord ⇒ r.get(name).asInstanceOf[B]
        case other                       ⇒ throw new com.mongodb.MongoException(s"DBObject expected but found ${other.getClass.getName}")
      }

    def innerJoin[E, C](f: T ⇒ Observable[E])(m: (T, E) ⇒ C): Observable[C] =
      self.flatMap { id ⇒ f(id).map(m(id, _)) }

    def innerJoinRaw[E](f: T ⇒ Observable[T])(m: (T, T) ⇒ E): Observable[E] =
      self.flatMap { id ⇒ f(id).map(m(id, _)) }
  }
}