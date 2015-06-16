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

import com.mongodb.{ BasicDBObject, MongoClient }
import mongo.query.MongoStream
import org.apache.log4j.Logger
import scala.reflect.ClassTag

package object join {
  import scala.language.higherKinds
  import mongo.dsl3._

  trait STypes {
    type Client = MongoClient
    type MStream[Out]
  }

  private val init = new BasicDBObject

  abstract class Joiner[T <: STypes] {
    //
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

    def left[A](q: Query.QueryBuilder[BasicDBObject], db: String, coll: String, keyColl: String): T#MStream[A]

    def relation[A, B](r: A ⇒ Query.QueryBuilder[BasicDBObject], db: String, collectionName: String): A ⇒ T#MStream[B]

    def innerJoin[A, B, C](l: T#MStream[A])(relation: A ⇒ T#MStream[B])(f: (A, B) ⇒ C): T#MStream[C]
  }

  trait MongoStreamsT extends STypes {
    type MStream[Out] = MongoStream[Client, Out]
  }

  object MongoStreamsT {
    import scalaz.Free.runFC
    import scalaz.concurrent.Task
    import scalaz.stream.process1.lift
    import scalaz.stream.{ Cause, io, Process }
    import Query.StatementOp
    val P = scalaz.stream.Process

    implicit object joiner extends Joiner[MongoStreamsT] {
      private def resource[T](q: BasicDBObject, client: MongoClient, db: String, coll: String): Process[Task, T] = {
        io.resource(Task.delay(client.getDB(db).getCollection(coll).find(q)))(c ⇒ Task.delay(c.close)) { c ⇒
          Task {
            if (c.hasNext) {
              val r = c.next
              log.info(s"fetch $r")
              r.asInstanceOf[T]
            } else throw Cause.Terminated(Cause.End)
          }(exec)
        }
      }

      override def left[A](q: Query.QueryBuilder[BasicDBObject], db: String, coll: String, keyColl: String): MongoStreamsT#MStream[A] = {
        val q0 = (runFC[StatementOp, QueryS, BasicDBObject](q)(Query.QueryInterpreterS)).run(init)._1
        log.info(s"[$db - $coll] query: $q0")
        MongoStream[MongoClient, A](P.eval(Task.now { client: MongoClient ⇒ Task(resource(q0, client, db, coll)) })).column[A](keyColl)
      }

      override def relation[A, B](r: A ⇒ Query.QueryBuilder[BasicDBObject], db: String, coll: String): A ⇒ MongoStreamsT#MStream[B] =
        id ⇒ {
          val q0 = (runFC[StatementOp, QueryS, BasicDBObject](r(id))(Query.QueryInterpreterS)).run(init)._1
          log.info(s"[$db - $coll] query $q0")
          MongoStream[MongoClient, B](P.eval(Task.now { client: MongoClient ⇒ Task(resource(q0, client, db, coll)) }))
        }

      override def innerJoin[A, B, C](l: MongoStreamsT#MStream[A])(relation: A ⇒ MongoStreamsT#MStream[B])(f: (A, B) ⇒ C): MongoStreamsT#MStream[C] =
        l.flatMap { id ⇒ relation(id) |> lift(f(id, _)) }
    }
  }

  object Joiner {
    def apply[T <: STypes](implicit j: Joiner[T], c: T#Client, log: Logger, pool: ExecutorService): Joiner[T] =
      j.withExecutor(pool).withLogger(log).withClient(c)
  }

  final class Join[T <: STypes: Joiner](implicit pool: ExecutorService, t: ClassTag[T], c: T#Client) {
    implicit val logger = Logger.getLogger(s"${t.runtimeClass.getName.dropWhile(_ != '$').drop(1)}-Joiner")
    private val joiner = Joiner[T]

    def join[A, B, C](lq: Query.QueryBuilder[BasicDBObject], lColl: String, keyColl: String,
                      rq: A ⇒ Query.QueryBuilder[BasicDBObject], rColl: String,
                      db: String)(f: (A, B) ⇒ C): T#MStream[C] = {
      logger.info("JoinProgram")
      joiner.innerJoin[A, B, C](joiner.left[A](lq, db, lColl, keyColl))(joiner.relation[A, B](rq, db, rColl))(f)
    }

  }
}
