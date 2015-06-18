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
  import mongo.dsl3.Query.StatementOp
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
    private val init: T#DBRecord = new com.mongodb.BasicDBObject

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

    /**
     * @param q
     * @return
     */
    def createQuery(q: QueryFree[T#DBRecord]) =
      scalaz.Free.runFC[StatementOp, QueryS, T#DBRecord](q)(Query.QueryInterpreterS).run(init)._1
    /**
     *
     * @param q
     * @param db
     * @param coll
     * @param keyField
     * @tparam A
     * @return
     */
    def left[A](q: Query.QueryFree[T#DBRecord], db: String, coll: String, keyField: String): T#DBStream[A]

    /**
     *
     * @param q
     * @param db
     * @param coll
     * @return
     */
    def leftR(q: Query.QueryFree[T#DBRecord], db: String, coll: String): T#DBStream[T#DBRecord]

    /**
     *
     * @param r
     * @param db
     * @param collectionName
     * @tparam A
     * @tparam B
     * @return
     */
    def relation[A, B](r: A ⇒ Query.QueryFree[T#DBRecord], db: String, collectionName: String): A ⇒ T#DBStream[B]

    /**
     *
     * @param r
     * @param db
     * @param collectionName
     * @return
     */
    def relationR(r: T#DBRecord ⇒ Query.QueryFree[T#DBRecord], db: String, collectionName: String): T#DBRecord ⇒ T#DBStream[T#DBRecord]

    /**
     *
     * @param l
     * @param relation
     * @param f
     * @tparam A
     * @tparam B
     * @tparam C
     * @return
     */
    def innerJoin[A, B, C](l: T#DBStream[A])(relation: A ⇒ T#DBStream[B])(f: (A, B) ⇒ C): T#DBStream[C]
  }

  object Joiner {
    def apply[T <: DBTypes](implicit j: Joiner[T], c: T#Client, log: Logger, pool: ExecutorService): Joiner[T] =
      j.withExecutor(pool).withLogger(log).withClient(c)
  }

  case class Join[T <: DBTypes: Joiner](implicit pool: ExecutorService, c: T#Client, t: ClassTag[T]) {

    implicit val logger = Logger.getLogger(s"${t.runtimeClass.getName.dropWhile(_ != '$').drop(1)}-Joiner")

    private val joiner = Joiner[T]

    /**
     * Performs inner join for 2 collections. It doesn't constraint your output type
     * @param leftQ Left stream query
     * @param leftCollection Left collection name
     * @param rightQ Right stream query
     * @param rightCollection Right collection name
     * @param db Function for transform result
     * @param f Function for transform result
     * @tparam A Type for output value
     * @return
     */
    def join[A](leftQ: QueryFree[T#DBRecord], leftCollection: String,
                rightQ: T#DBRecord ⇒ QueryFree[T#DBRecord], rightCollection: String,
                db: String)(f: (T#DBRecord, T#DBRecord) ⇒ A): T#DBStream[A] =
      joiner.innerJoin[T#DBRecord, T#DBRecord, A](joiner.leftR(leftQ, db, leftCollection))(joiner.relationR(rightQ, db, rightCollection))(f)

    /**
     * Performs inner join for 2 collections. Allows you to pass field name for left stream. That value will be passed in right query.
     * It constraints your output type with key field from left stream and any fields from right stream.
     * @param leftQ Left stream query
     * @param leftCollection Left collection name
     * @param key Left stream key column
     * @param rightQ Right stream query
     * @param rightCollection Right collection name
     * @param db Collection's db name
     * @param f  Function for transform result
     * @tparam A Type for element from left stream
     * @tparam B Type for element from right stream
     * @tparam C Type for output value
     * @return
     */
    def joinByPk[A, B, C](leftQ: QueryFree[T#DBRecord], leftCollection: String, key: String,
                          rightQ: A ⇒ QueryFree[T#DBRecord], rightCollection: String,
                          db: String)(f: (A, B) ⇒ C): T#DBStream[C] =
      joiner.innerJoin[A, B, C](joiner.left[A](leftQ, db, leftCollection, key))(joiner.relation[A, B](rightQ, db, rightCollection))(f)
  }
}