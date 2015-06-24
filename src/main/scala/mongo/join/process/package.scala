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
  import mongo.dsl.qb.QueryFree

  trait ProcessStream extends DBModule {
    override type DBStream[Out] = DBChannel[Client, Out]
  }

  object ProcessStream {
    import scalaz.concurrent.Task
    import scalaz.stream.process1.lift
    import scalaz.stream.{ Cause, io, Process }
    val P = scalaz.stream.Process

    implicit object joiner extends Joiner[ProcessStream] {
      private def resource[T](qs: ProcessStream#QuerySettings, client: ProcessStream#Client, db: String, collection: String): Process[Task, T] = {
        io.resource(Task.delay {
          val coll = client.getDB(db).getCollection(collection)
          val cursor = coll.find(qs.q)
          qs.sort.foreach(cursor.sort(_))
          qs.skip.foreach(cursor.skip(_))
          qs.limit.foreach(cursor.limit(_))
          log.debug(s"Query-settings: Sort:[ ${qs.sort} ] Skip:[ ${qs.skip} ] Limit:[ ${qs.limit} ] Query:[ ${qs.q} ]")
          cursor
        })(c ⇒ Task.delay(c.close)) { c ⇒
          Task {
            if (c.hasNext) {
              val r = c.next
              log.debug(s"fetch $r")
              r.asInstanceOf[T]
            } else throw Cause.Terminated(Cause.End)
          }(exec)
        }
      }

      override def leftField[A](q: QueryFree[ProcessStream#QuerySettings], db: String, coll: String, keyColl: String): ProcessStream#DBStream[A] =
        DBChannel[ProcessStream#Client, A](P.eval(Task.now { client: ProcessStream#Client ⇒
          Task(resource(createQuery(q), client, db, coll))
        })).column[A](keyColl)

      override def left(q: QueryFree[ProcessStream#QuerySettings], db: String, coll: String): DBChannel[ProcessStream#Client, ProcessStream#DBRecord] =
        DBChannel[ProcessStream#Client, ProcessStream#DBRecord](P.eval(Task.now { client: ProcessStream#Client ⇒
          Task(resource(createQuery(q), client, db, coll))
        }))

      override def relationField[A, B](r: A ⇒ QueryFree[ProcessStream#QuerySettings], db: String, coll: String): A ⇒ ProcessStream#DBStream[B] =
        id ⇒
          DBChannel[ProcessStream#Client, B](P.eval(Task.now { client: ProcessStream#Client ⇒
            Task(resource(createQuery(r(id)), client, db, coll))
          }))

      override def relation(r: (ProcessStream#DBRecord) ⇒ QueryFree[ProcessStream#QuerySettings], db: String, coll: String): (ProcessStream#DBRecord) ⇒ DBChannel[ProcessStream#Client, ProcessStream#DBRecord] =
        topRecord ⇒
          DBChannel[ProcessStream#Client, ProcessStream#DBRecord](P.eval(Task.now { client: ProcessStream#Client ⇒
            Task(resource(createQuery(r(topRecord)), client, db, coll))
          }))

      override def innerJoin[A, B, C](l: ProcessStream#DBStream[A])(relation: A ⇒ ProcessStream#DBStream[B])(f: (A, B) ⇒ C): ProcessStream#DBStream[C] =
        for {
          id ← l
          rs ← relation(id) |> lift(f(id, _))
        } yield rs
    }
  }
}
