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

import com.mongodb.{ DBCursor, DBObject, BasicDBObject, MongoClient }

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scalaz.concurrent.Task
import scalaz.{ Free, Coyoneda, \/, -\/, \/-, Trampoline, ~> }

import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConversions.mapAsScalaMap

package object dsl3 { outer ⇒

  object FetchMode extends Enumeration {
    type Type = Value
    val One, Batch = Value
  }

  type QueryS[T] = scalaz.State[BasicDBObject, T]

  sealed trait LogLevel
  case object ErrorLevel extends LogLevel
  case object WarnLevel extends LogLevel
  case object InfoLevel extends LogLevel
  case object DebugLevel extends LogLevel

  sealed trait Log[A]
  case class LogMsg(level: LogLevel, msg: String) extends Log[Unit]

  object Query {
    import shapeless._
    import shapeless.CNil
    import CoyonedaShapless._

    sealed trait StatementOp[T]
    case class EqQ(q: BasicDBObject) extends StatementOp[BasicDBObject]
    case class ChainQ(q: BasicDBObject) extends StatementOp[BasicDBObject]
    type QueryBuilder[A] = scalaz.Free.FreeC[StatementOp, A]

    type QueryApp[T] = StatementOp[T] :+: Log[T] :+: CNil
    type CoyoApp[T] = Coyoneda[QueryApp, T]
    type FreeApp[T] = scalaz.Free.FreeC[QueryApp, T]

    object LogQuery {
      def debug(msg: String) = Copoyo[QueryApp](LogMsg(DebugLevel, msg))
    }

    private val init = new BasicDBObject

    implicit def f2FreeM(q: mongo.EqQueryFragment): QueryBuilder[BasicDBObject] = scalaz.Free.liftFC(EqQ(q.q))
    implicit def c2FreeM(q: mongo.ComposableQueryFragment): QueryBuilder[BasicDBObject] = scalaz.Free.liftFC(ChainQ(q.q))

    type QueryS[T] = scalaz.State[BasicDBObject, T]
    object QueryInterpreterS extends (Query.StatementOp ~> QueryS) {
      def apply[T](op: Query.StatementOp[T]): QueryS[T] = op match {
        case Query.EqQ(q) ⇒
          scalaz.State { (in: BasicDBObject) ⇒
            (new BasicDBObject(mapAsJavaMap(mapAsScalaMap(in.toMap) ++ mapAsScalaMap(q.toMap))), in)
          }
        case Query.ChainQ(q) ⇒
          scalaz.State { (in: BasicDBObject) ⇒
            (new BasicDBObject(mapAsJavaMap(mapAsScalaMap(in.toMap) ++ mapAsScalaMap(q.toMap))), in)
          }
      }
    }

    def query(rq: QueryBuilder[BasicDBObject]): FreeApp[BasicDBObject] =
      for {
        _ ← LogQuery.debug(s"Incoming query")
        q = (scalaz.Free.runFC[StatementOp, QueryS, BasicDBObject](rq)(QueryInterpreterS))
          .run(init)._1
        _ ← LogQuery.debug(s"Query: ${q.toString}")
      } yield q
  }

  object Interaction {
    import Free._
    import shapeless._
    import shapeless.CNil
    import ops.coproduct.Inject
    import CoyonedaShapless._

    sealed trait DBError
    case object NotFound extends DBError
    case class ReadError(msg: String) extends DBError

    type NonEmptyResult = DBError \/ DBObject

    sealed trait MongoInteractionOp[T]
    case class ReadOne(client: MongoClient, q: BasicDBObject, db: String, coll: String) extends MongoInteractionOp[NonEmptyResult]
    case class ReadBatch(client: MongoClient, q: BasicDBObject, db: String, coll: String) extends MongoInteractionOp[NonEmptyResult]

    type InteractionApp[T] = MongoInteractionOp[T] :+: Log[T] :+: Query.FreeApp[T] :+: CNil
    type CoyoApp[T] = Coyoneda[InteractionApp, T]
    type FreeApp[T] = scalaz.Free.FreeC[InteractionApp, T]

    val BatchPrefix = "_id"

    def lift[F[_], A](a: F[A])(implicit inj: Inject[InteractionApp[A], F[A]]): FreeApp[A] = Copoyo[InteractionApp](a)

    def readOne(client: MongoClient, q: BasicDBObject, db: String, coll: String): FreeApp[NonEmptyResult] =
      lift(ReadOne(client, q, db, coll))

    def readBatch(client: MongoClient, q: BasicDBObject, db: String, coll: String): FreeApp[NonEmptyResult] =
      lift(ReadBatch(client, q, db, coll))

    def createQuery(rq: Query.QueryBuilder[BasicDBObject]): FreeApp[BasicDBObject] =
      lift(Query.query(rq))

    object LogInteraction {
      def debug(msg: String) = Copoyo[InteractionApp](LogMsg(DebugLevel, msg))
    }

    def program(rq: Query.QueryBuilder[BasicDBObject], client: MongoClient,
                db: String, coll: String, mode: FetchMode.Type): FreeApp[NonEmptyResult] =
      for {
        q ← createQuery(rq)
        r ← if (mode == FetchMode.One) readOne(client, q, db, coll)
        else readBatch(client, q, db, coll)
        _ ← LogInteraction.debug(s"Fetched result: ${r}")
      } yield (r)

    object ApacheLog4j extends (Log ~> Id) {
      val logger = org.apache.log4j.Logger.getLogger("mongo-query")
      def apply[A](a: Log[A]) = a match {
        case LogMsg(lvl, msg) ⇒ logger.debug(s"$lvl: $msg")
      }
    }

    object QueryInterpreter extends (Query.StatementOp ~> Id) {
      override def apply[A](fa: Query.StatementOp[A]): Id[A] = fa match {
        case other ⇒ throw new Exception("This is Davy John locker")
      }
    }

    private object Trampolined extends (Id ~> Trampoline) {
      def apply[A](a: A): Trampoline[A] = Trampoline.done(a)
    }

    object InteractionInterpreter extends (MongoInteractionOp ~> Id) {
      @tailrec def go[A](cursor: DBCursor, list: List[A]): List[A] =
        if (cursor.hasNext) {
          val r = cursor.next.asInstanceOf[A]
          go(cursor, r :: list)
        } else list

      override def apply[A](fa: MongoInteractionOp[A]): Id[A] = fa match {
        case f @ ReadOne(client, q, db, coll) ⇒
          val r = client.getDB(db).getCollection(coll).findOne(q)
          if (r == null) -\/(NotFound) else \/-(r)
        case f @ ReadBatch(client, q, db, coll) ⇒ {
          val cursor = client.getDB(db).getCollection(coll).find(q)
          try {
            \/-(new BasicDBObject(BatchPrefix, seqAsJavaList(go(cursor, Nil))))
          } catch {
            case e: Exception ⇒ -\/(ReadError(e.getMessage))
          } finally {
            cursor.close
          }
        }
      }
    }

    val qInterpreter: Query.QueryApp ~> Id = QueryInterpreter ||: ApacheLog4j
    val qInterpreterCoyo: Query.CoyoApp ~> Id = liftCoyoLeft(qInterpreter)
    val qInterpreterFree: Query.FreeApp ~> Id = liftFree(qInterpreterCoyo)

    val intInterpreter: Interaction.InteractionApp ~> Id = InteractionInterpreter ||: ApacheLog4j ||: qInterpreterFree
    val intInterpreterCoyo: Interaction.CoyoApp ~> Id = liftCoyoLeft(intInterpreter)

    implicit class ProgramSyntax(val self: Query.QueryBuilder[BasicDBObject]) extends AnyVal {
      def one(client: MongoClient, db: String, coll: String)(implicit pool: java.util.concurrent.ExecutorService) =
        Task(outer.Interaction.program(self, client, db, coll, FetchMode.One).foldMap(Trampolined compose intInterpreterCoyo).run)(pool)

      def list(client: MongoClient, db: String, coll: String)(implicit pool: java.util.concurrent.ExecutorService): Task[NonEmptyResult] =
        Task(outer.Interaction.program(self, client, db, coll, FetchMode.Batch).foldMap(Trampolined compose intInterpreterCoyo).run)(pool)
    }
  }
}
