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
import mongo.dsl3.Interaction.StreamerFactory
import rx.lang.scala.schedulers.ExecutionContextScheduler
import rx.lang.scala.{ Producer, Observable, Subscriber }
import com.mongodb._

import scala.concurrent.ExecutionContext
import scala.util.Try
import scala.annotation.tailrec

package object observable {

  implicit val M = new scalaz.Monad[Observable]() {
    override def point[A](a: ⇒ A): Observable[A] = Observable.just(a)
    override def bind[A, B](fa: Observable[A])(f: (A) ⇒ Observable[B]): Observable[B] = fa.flatMap(f)
  }

  implicit object RxStreamer extends StreamerFactory[Observable] {
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

  implicit class ObservableSyntax[T](val self: Observable[T]) extends AnyVal {

    def column[B](name: String): Observable[B] =
      self.map { record ⇒
        record match {
          case r: DBObject ⇒ r.get(name).asInstanceOf[B]
          case other       ⇒ throw new MongoException(s"DBObject expected but found ${other.getClass.getName}")
        }
      }

    def leftJoin[E, C](f: T ⇒ Observable[E])(m: (T, E) ⇒ C): Observable[C] =
      self.flatMap { id ⇒ f(id).map(m(id, _)) }

    def leftJoinRaw[E](f: T ⇒ Observable[T])(m: (T, T) ⇒ E): Observable[E] =
      self.flatMap { id ⇒ f(id).map(m(id, _)) }
  }
}
