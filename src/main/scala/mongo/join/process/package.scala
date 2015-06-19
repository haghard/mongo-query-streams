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
  import mongo.query.DBChannel
  import mongo.dsl3.Query.QueryFree

  trait ProcessS extends DBTypes {
    override type DBStream[Out] = DBChannel[Client, Out]
  }

  object ProcessS {
    import scalaz.concurrent.Task
    import scalaz.stream.process1.lift
    import scalaz.stream.{ Cause, io, Process }
    val P = scalaz.stream.Process

    implicit object joiner extends Joiner[ProcessS] {
      type Record = ProcessS#DBRecord
      val init: Record = new com.mongodb.BasicDBObject

      private def resource[T](q: Record, client: ProcessS#Client, db: String, coll: String): Process[Task, T] = {
        log.info(s"[$db - $coll] query: $q")
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

      override def leftField[A](q: QueryFree[Record], db: String, coll: String, keyColl: String): ProcessS#DBStream[A] =
        DBChannel[ProcessS#Client, A](P.eval(Task.now { client: ProcessS#Client ⇒
          Task(resource(createQuery(q), client, db, coll))
        })).column[A](keyColl)

      override def left(q: QueryFree[Record], db: String, coll: String): DBChannel[ProcessS#Client, ProcessS#DBRecord] =
        DBChannel[ProcessS#Client, Record](P.eval(Task.now { client: ProcessS#Client ⇒
          Task(resource(createQuery(q), client, db, coll))
        }))

      override def relationField[A, B](r: A ⇒ QueryFree[Record], db: String, coll: String): A ⇒ ProcessS#DBStream[B] =
        id ⇒
          DBChannel[ProcessS#Client, B](P.eval(Task.now { client: ProcessS#Client ⇒
            Task(resource(createQuery(r(id)), client, db, coll))
          }))

      override def relation(r: (Record) ⇒ QueryFree[Record], db: String, coll: String): (ProcessS#DBRecord) ⇒ DBChannel[ProcessS#Client, ProcessS#DBRecord] =
        topRecord ⇒
          DBChannel[ProcessS#Client, Record](P.eval(Task.now { client: ProcessS#Client ⇒
            Task(resource(createQuery(r(topRecord)), client, db, coll))
          }))

      override def innerJoin[A, B, C](l: ProcessS#DBStream[A])(relation: A ⇒ ProcessS#DBStream[B])(f: (A, B) ⇒ C): ProcessS#DBStream[C] =
        for {
          id ← l
          rs ← relation(id) |> lift(f(id, _))
        } yield rs
    }
  }
}
