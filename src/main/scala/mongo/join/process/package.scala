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

package object process {
  import mongo.dsl3._
  import mongo.query.DBChannel
  import mongo.dsl3.QueryS
  import mongo.dsl3.Query.QueryFree

  trait ProcessS extends DBTypes {
    type DBStream[Out] = DBChannel[Client, Out]
  }

  object ProcessS {
    import scalaz.Free.runFC
    import scalaz.concurrent.Task
    import scalaz.stream.{ Cause, io, Process }
    import Query.StatementOp
    import scalaz.stream.process1.lift
    val P = scalaz.stream.Process

    implicit object joiner extends Joiner[ProcessS] {
      val init: ProcessS#DBRecord = new com.mongodb.BasicDBObject

      private def resource[T](q: ProcessS#DBRecord, client: ProcessS#Client, db: String, coll: String): Process[Task, T] = {
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

      private def createQuery(q: QueryFree[ProcessS#DBRecord]) =
        (runFC[StatementOp, QueryS, ProcessS#DBRecord](q)(Query.QueryInterpreterS)).run(init)._1

      override def left[A](q: QueryFree[ProcessS#DBRecord], db: String, coll: String, keyColl: String): ProcessS#DBStream[A] = {
        val query = createQuery(q)
        log.info(s"[$db - $coll] query: $query")
        DBChannel[ProcessS#Client, A](P.eval(Task.now { client: ProcessS#Client ⇒ Task(resource(query, client, db, coll)) })).column[A](keyColl)
      }

      override def relation[A, B](r: A ⇒ QueryFree[ProcessS#DBRecord], db: String, coll: String): A ⇒ ProcessS#DBStream[B] =
        id ⇒ {
          val query = createQuery(r(id))
          log.info(s"[$db - $coll] query $query")
          DBChannel[ProcessS#Client, B](P.eval(Task.now { client: ProcessS#Client ⇒ Task(resource(query, client, db, coll)) }))
        }

      //l.flatMap { id ⇒ relation(id) |> lift(f(id, _)) }
      override def innerJoin[A, B, C](l: ProcessS#DBStream[A])(relation: A ⇒ ProcessS#DBStream[B])(f: (A, B) ⇒ C): ProcessS#DBStream[C] = {
        for {
          id ← l
          rs ← relation(id) |> lift(f(id, _))
        } yield rs
      }
    }
  }
}
