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

package join

package object process {
  import mongo.query.DBChannel
  import mongo.dsl.qb.QueryFree

  trait MongoProcessStream extends MongoDBModule {
    override type DBStream[Out] = DBChannel[Client, Out]
  }

  object MongoProcessStream {
    import scalaz.concurrent.Task
    import scalaz.stream.process1.lift
    import scalaz.stream.{ Cause, io, Process }
    val P = scalaz.stream.Process

    implicit object joiner extends Joiner[MongoProcessStream] {
      private def resource[T](qs: MongoProcessStream#QuerySettings, client: MongoProcessStream#Client, db: String, collection: String): Process[Task, T] = {
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

      override def leftField[A](q: QueryFree[MongoProcessStream#QuerySettings], db: String, coll: String, keyColl: String): MongoProcessStream#DBStream[A] =
        DBChannel[MongoProcessStream#Client, A](P.eval(Task.now { client: MongoProcessStream#Client ⇒
          Task(resource(createQuery(q), client, db, coll))
        })).column[A](keyColl)

      override def left(q: QueryFree[MongoProcessStream#QuerySettings], db: String, coll: String): DBChannel[MongoProcessStream#Client, MongoProcessStream#DBRecord] =
        DBChannel[MongoProcessStream#Client, MongoProcessStream#DBRecord](P.eval(Task.now { client: MongoProcessStream#Client ⇒
          Task(resource(createQuery(q), client, db, coll))
        }))

      override def relationField[A, B](r: A ⇒ QueryFree[MongoProcessStream#QuerySettings], db: String, coll: String): A ⇒ MongoProcessStream#DBStream[B] =
        id ⇒
          DBChannel[MongoProcessStream#Client, B](P.eval(Task.now { client: MongoProcessStream#Client ⇒
            Task(resource(createQuery(r(id)), client, db, coll))
          }))

      override def relation(r: (MongoProcessStream#DBRecord) ⇒ QueryFree[MongoProcessStream#QuerySettings], db: String, coll: String): (MongoProcessStream#DBRecord) ⇒ DBChannel[MongoProcessStream#Client, MongoProcessStream#DBRecord] =
        topRecord ⇒
          DBChannel[MongoProcessStream#Client, MongoProcessStream#DBRecord](P.eval(Task.now { client: MongoProcessStream#Client ⇒
            Task(resource(createQuery(r(topRecord)), client, db, coll))
          }))

      override def innerJoin[A, B, C](l: MongoProcessStream#DBStream[A])(relation: A ⇒ MongoProcessStream#DBStream[B])(f: (A, B) ⇒ C): MongoProcessStream#DBStream[C] =
        for {
          id ← l
          rs ← relation(id) |> lift(f(id, _))
        } yield rs
    }
  }
}
