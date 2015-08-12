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
      def c: MongoObservable#Client
      def log: Logger
      def setting: MongoObservable#QuerySettings
      def subscriber: Subscriber[T]

      private val pageSize = 8

      lazy val cursor: Option[MongoObservable#Cursor] = (Try {
        Option {
          import scalaz.syntax.id._
          val cursor = c.getDB(db).getCollection(coll).find(setting.q)
          cursor |> { c ⇒
            setting.sort.foreach(c.sort)
            setting.limit.foreach(c.limit)
            setting.skip.foreach(c.skip)
          }
          log.debug(s"Query-settings: Sort:[ ${setting.sort} ] Skip:[ ${setting.skip} ] Limit:[ ${setting.limit} ] Query:[ ${setting.q} ]")
          cursor
        }
      } recover {
        case e: Throwable ⇒
          subscriber.onError(e)
          None
      }).get

      @tailrec private def fetch(n: Int, i: Int, c: MongoObservable#Cursor): Unit = {
        if (i < n && c.hasNext && !subscriber.isUnsubscribed) {
          val r = extract(c)
          subscriber.onNext(r)
          fetch(n, i + 1, c)
        }
      }

      def extract(c: MongoObservable#Cursor): T = {
        val r = c.next.asInstanceOf[T]
        log.info(s"fetch $r")
        r
      }

      def producer: (Long) ⇒ Unit =
        n ⇒ {
          val intN = if (n >= pageSize) pageSize else n.toInt
          //log.info(s"★ Request:$n $intN ★")
          if (cursor.isDefined) {
            if (cursor.get.hasNext) fetch(intN, 0, cursor.get)
            else subscriber.onCompleted()
          }
        }
    }

    trait PKFetcher[T] extends Fetcher[T] {
      def key: String
      abstract override def extract(c: MongoObservable#Cursor): T =
        super.extract(c).asInstanceOf[MongoObservable#DBRecord].get(key).asInstanceOf[T]
    }

    implicit object joiner extends Joiner[MongoObservable] {
      val scheduler = ExecutionContextScheduler(ExecutionContext.fromExecutor(exec))

      private def resource[A](qs: MongoObservable#QuerySettings, db0: String, c0: String): Observable[A] = {
        logger.info(s"[$db0 - $c0] Query: $qs")
        Observable { subscriber0: Subscriber[A] ⇒
          subscriber0.setProducer(new Fetcher[A] {
            override def db = db0
            override def subscriber = subscriber0
            override def log = logger
            override def coll = c0
            override def setting = qs
            override def c = client
          }.producer)
        }.subscribeOn(scheduler)
      }

      private def resource2[A](qs: MongoObservable#QuerySettings, db0: String, c0: String, keyField: String): Observable[A] = {
        logger.info(s"[$db0 - $c0] Query: $qs")
        Observable { subscriber0: Subscriber[A] ⇒
          subscriber0.setProducer(new PKFetcher[A] {
            override def key = keyField
            override def subscriber = subscriber0
            override def log = logger
            override def coll = c0
            override def db = db0
            override def setting = qs
            override def c = client
          }.producer)
        }.subscribeOn(scheduler)
      }

      override def leftField[A](query: QueryFree[MongoObservable#QuerySettings],
                                db: String, collection: String, key: String): MongoObservable#DBStream[A] =
        resource2[A](createQuery(query), db, collection, key)

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

      override def innerJoin[A, B, C](outer: Observable[A])(relation: (A) ⇒ Observable[B])(f: (A, B) ⇒ C): MongoObservable#DBStream[C] =
        for {
          id ← outer
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