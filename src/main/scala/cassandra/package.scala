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

import mongo.dsl.qb.QueryFree
import mongo.query.DBChannel
import rx.lang.scala.Observable
import scala.concurrent.ExecutionContext
import rx.lang.scala.schedulers.ExecutionContextScheduler

package object cassandra {

  case class CassandraParamValue(name: String, v: AnyRef, clazz: Class[_])
  case class CassandraQuerySettings(val q: String, val v: Option[CassandraParamValue] = None)

  trait CassandraDBModule extends join.DBModule {
    override type Client = com.datastax.driver.core.Cluster
    override type DBRecord = com.datastax.driver.core.Row
    override type QuerySettings = CassandraQuerySettings
    override type Cursor = java.util.Iterator[com.datastax.driver.core.Row]
  }

  trait CassandraObservableStream extends CassandraDBModule {
    override type DBStream[Out] = rx.lang.scala.Observable[Out]
  }

  trait CassandraProcessStream extends CassandraDBModule {
    override type DBStream[Out] = DBChannel[Client, Out]
  }

  implicit object joiner extends join.Joiner[CassandraObservableStream] {
    val scheduler = ExecutionContextScheduler(ExecutionContext.fromExecutor(exec))

    /**
     *
     * @param q
     * @param db
     * @param coll
     * @param keyField
     * @tparam A
     * @return
     */
    override def leftField[A](q: QueryFree[CassandraObservableStream#QuerySettings], db: String, coll: String, keyField: String): Observable[A] = ???

    /**
     *
     * @param r
     * @param db
     * @param collectionName
     * @return
     */
    override def relation(r: (CassandraObservableStream#DBRecord) ⇒ QueryFree[CassandraObservableStream#QuerySettings], db: String, collectionName: String): (CassandraObservableStream#DBRecord) ⇒ Observable[CassandraObservableStream#DBRecord] = ???

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
    override def innerJoin[A, B, C](l: Observable[A])(relation: (A) ⇒ Observable[B])(f: (A, B) ⇒ C): Observable[C] = ???

    /**
     *
     * @param q
     * @param db
     * @param coll
     * @return
     */
    override def left(q: QueryFree[CassandraObservableStream#QuerySettings], db: String, coll: String): Observable[CassandraObservableStream#DBRecord] = ???

    /**
     *
     * @param r
     * @param db
     * @param collectionName
     * @tparam A
     * @tparam B
     * @return
     */
    override def relationField[A, B](r: (A) ⇒ QueryFree[CassandraObservableStream#QuerySettings], db: String, collectionName: String): (A) ⇒ Observable[B] = ???
  }
}