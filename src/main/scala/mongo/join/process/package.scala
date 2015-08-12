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

  trait MongoProcess extends DBModule {
    override type Client = com.mongodb.MongoClient
    override type DBRecord = com.mongodb.DBObject
    override type QuerySettings = mongo.dsl.QuerySettings
    override type Cursor = com.mongodb.Cursor
    override type DBStream[Out] = DBChannel[Client, Out]
  }

  object MongoProcess {
    import scalaz.concurrent.Task
    import scalaz.stream.process1.lift
    import scalaz.stream.{ Cause, io, Process }
    val P = scalaz.stream.Process

    implicit object joiner extends Joiner[MongoProcess] {
      private def resource[T](qs: MongoProcess#QuerySettings, client: MongoProcess#Client,
                              db: String, collection: String): Process[Task, T] = {
        io.resource(Task.delay {
          val coll = client.getDB(db).getCollection(collection)
          val cursor = coll.find(qs.q)
          qs.sort.foreach(q ⇒ cursor.sort(q))
          qs.skip.foreach(n ⇒ cursor.skip(n))
          qs.limit.foreach(n ⇒ cursor.limit(n))
          logger.debug(s"Query-settings: Sort:[ ${qs.sort} ] Skip:[ ${qs.skip} ] Limit:[ ${qs.limit} ] Query:[ ${qs.q} ]")
          cursor
        })(c ⇒ Task.delay(c.close())) { c ⇒
          Task {
            if (c.hasNext) {
              val r = c.next
              logger.debug(s"fetch $r")
              r.asInstanceOf[T]
            } else throw Cause.Terminated(Cause.End)
          }(exec)
        }
      }

      override def leftField[A](q: QueryFree[MongoProcess#QuerySettings],
                                db: String, collection: String, keyColl: String): MongoProcess#DBStream[A] =
        DBChannel[MongoProcess#Client, A](P.eval(Task.now { client: MongoProcess#Client ⇒
          Task(resource(createQuery(q), client, db, collection))
        })).column[A](keyColl)

      override def left(q: QueryFree[MongoProcess#QuerySettings],
                        db: String, collection: String): DBChannel[MongoProcess#Client, MongoProcess#DBRecord] =
        DBChannel[MongoProcess#Client, MongoProcess#DBRecord](P.eval(Task.now { client: MongoProcess#Client ⇒
          Task(resource(createQuery(q), client, db, collection))
        }))

      override def relationField[A, B](r: A ⇒ QueryFree[MongoProcess#QuerySettings],
                                       db: String, collection: String): A ⇒ MongoProcess#DBStream[B] =
        id ⇒
          DBChannel[MongoProcess#Client, B](P.eval(Task.now { client: MongoProcess#Client ⇒
            Task(resource(createQuery(r(id)), client, db, collection))
          }))

      override def relation(r: (MongoProcess#DBRecord) ⇒ QueryFree[MongoProcess#QuerySettings],
                            db: String, collection: String): (MongoProcess#DBRecord) ⇒ DBChannel[MongoProcess#Client, MongoProcess#DBRecord] =
        topRecord ⇒
          DBChannel[MongoProcess#Client, MongoProcess#DBRecord](P.eval(Task.now { client: MongoProcess#Client ⇒
            Task(resource(createQuery(r(topRecord)), client, db, collection))
          }))

      override def innerJoin[A, B, C](outer: MongoProcess#DBStream[A])(relation: A ⇒ MongoProcess#DBStream[B])(f: (A, B) ⇒ C): MongoProcess#DBStream[C] =
        for {
          id ← outer
          rs ← relation(id) |> lift(f(id, _))
        } yield rs
    }
  }
}
