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

import mongo.dsl.qb.QueryS
import scala.reflect.ClassTag
import org.apache.log4j.Logger
import scala.language.higherKinds
import java.util.concurrent.ExecutorService

/**
 *
 * Represent abstract way for doing join
 *
 */
package object join {
  import mongo.dsl._
  import mongo.dsl.qb.StatementOp
  import mongo.dsl.qb.QueryFree

  /**
   * Base abstraction for types in Join domain
   */
  trait DBModule {
    type DBRecord
    type QuerySettings
    type Cursor
    type Client

    type DBStream[A] <: {
      def map[B](f: A ⇒ B): DBStream[B]
      def flatMap[B](f: A ⇒ DBStream[B]): DBStream[B]
    }
  }

  /**
   * Base abstraction for methods in Join domain
   * @tparam T
   */
  abstract class Joiner[T <: DBModule] {
    protected var log: Logger = _
    protected var client: T#Client = _
    protected var exec: ExecutorService = _
    private val initQ = new com.mongodb.BasicDBObject
    private val init = QuerySettings(initQ)

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
    protected def createQuery(q: QueryFree[T#QuerySettings]): QuerySettings =
      scalaz.Free.runFC[StatementOp, QueryS, T#QuerySettings](q)(qb.QueryInterpreter).run(init)._1

    /**
     *
     * @param query
     * @param db
     * @param collection
     * @param keyField
     * @tparam A
     * @return
     */
    def leftField[A](query: qb.QueryFree[T#QuerySettings], db: String, collection: String, keyField: String): T#DBStream[A]

    /**
     *
     * @param query
     * @param db
     * @param collection
     * @return
     */
    def left(query: qb.QueryFree[T#QuerySettings], db: String, collection: String): T#DBStream[T#DBRecord]

    /**
     *
     * @param relation
     * @param db
     * @param collection
     * @tparam A
     * @tparam B
     * @return A ⇒ T#DBStream[B]
     */
    def relationField[A, B](relation: A ⇒ qb.QueryFree[T#QuerySettings], db: String, collection: String): A ⇒ T#DBStream[B]

    /**
     *
     * @param relation
     * @param db
     * @param collection
     * @return T#DBRecord ⇒ T#DBStream[T#DBRecord]
     */
    def relation(relation: T#DBRecord ⇒ qb.QueryFree[T#QuerySettings], db: String, collection: String): T#DBRecord ⇒ T#DBStream[T#DBRecord]

    /**
     *
     * @param l
     * @param relation
     * @param f
     * @tparam A
     * @tparam B
     * @tparam C
     * @return T#DBStream[C]
     */
    def innerJoin[A, B, C](l: T#DBStream[A])(relation: A ⇒ T#DBStream[B])(f: (A, B) ⇒ C): T#DBStream[C]
  }

  object Joiner {
    def apply[T <: DBModule](implicit j: Joiner[T], c: T#Client, log: Logger, pool: ExecutorService): Joiner[T] =
      j.withExecutor(pool).withLogger(log).withClient(c)
  }

  case class Join[T <: DBModule: Joiner](implicit pool: ExecutorService, c: T#Client, t: ClassTag[T]) {

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
     * @return T#DBStream[A]
     */
    def join[A](leftQ: QueryFree[T#QuerySettings], leftCollection: String,
                rightQ: T#DBRecord ⇒ QueryFree[T#QuerySettings], rightCollection: String,
                db: String)(f: (T#DBRecord, T#DBRecord) ⇒ A): T#DBStream[A] =
      joiner.innerJoin[T#DBRecord, T#DBRecord, A](joiner.left(leftQ, db, leftCollection))(joiner.relation(rightQ, db, rightCollection))(f)

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
     * @return T#DBStream[C]
     */
    def joinByPk[A, B, C](leftQ: QueryFree[T#QuerySettings], leftCollection: String, key: String,
                          rightQ: A ⇒ QueryFree[T#QuerySettings], rightCollection: String,
                          db: String)(f: (A, B) ⇒ C): T#DBStream[C] =
      joiner.innerJoin[A, B, C](joiner.leftField[A](leftQ, db, leftCollection, key))(joiner.relationField[A, B](rightQ, db, rightCollection))(f)
  }
}