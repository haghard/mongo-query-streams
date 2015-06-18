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

import java.util.concurrent.ExecutorService
import org.apache.log4j.Logger
import scala.reflect.ClassTag
import scala.language.higherKinds

package object join {
  import mongo.dsl3._
  import mongo.dsl3.Query.QueryFree

  trait DBTypes {
    type Client = com.mongodb.MongoClient
    type DBRecord = com.mongodb.BasicDBObject
    type Cursor = com.mongodb.Cursor
    type DBStream[A] <: {
      def map[B](f: A ⇒ B): DBStream[B]
      def flatMap[B](f: A ⇒ DBStream[B]): DBStream[B]
    }
  }

  abstract class Joiner[T <: DBTypes] {
    protected var log: Logger = null
    protected var client: T#Client = null
    protected var exec: ExecutorService = null

    def withClient(client: T#Client) = {
      this.client = client
      this
    }

    def withLogger(log: Logger) = {
      this.log = log
      this
    }

    def withExecutor(ex: ExecutorService): Joiner[T] = {
      exec = ex
      this
    }

    def left[A](q: Query.QueryFree[T#DBRecord], db: String, coll: String, keyColl: String): T#DBStream[A]

    def relation[A, B](r: A ⇒ Query.QueryFree[T#DBRecord], db: String, collectionName: String): A ⇒ T#DBStream[B]

    def innerJoin[A, B, C](l: T#DBStream[A])(relation: A ⇒ T#DBStream[B])(f: (A, B) ⇒ C): T#DBStream[C]
  }

  object Joiner {
    def apply[T <: DBTypes](implicit j: Joiner[T], c: T#Client, log: Logger, pool: ExecutorService): Joiner[T] =
      j.withExecutor(pool).withLogger(log).withClient(c)
  }

  case class Join[T <: DBTypes: Joiner](implicit pool: ExecutorService, c: T#Client, t: ClassTag[T]) {
    implicit val logger = Logger.getLogger(s"${t.runtimeClass.getName.dropWhile(_ != '$').drop(1)}-Joiner")

    private val joiner = Joiner[T]

    def join[A, B, C](left: QueryFree[T#DBRecord], leftC: String, key: String,
                      right: A ⇒ QueryFree[T#DBRecord], rightC: String,
                      db: String)(f: (A, B) ⇒ C): T#DBStream[C] = {
      logger.info("Join-Program")
      joiner.innerJoin[A, B, C](joiner.left[A](left, db, leftC, key))(joiner.relation[A, B](right, db, rightC))(f)
    }
  }
}