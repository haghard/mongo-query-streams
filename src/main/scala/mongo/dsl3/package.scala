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

import mongo.dsl3.Query.QueryS
import mongo.query.DBChannel
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scalaz.concurrent.Task
import java.util.concurrent.ExecutorService
import scalaz.{ Free, Coyoneda, \/, -\/, \/-, Trampoline, ~> }
import com.mongodb.{ DBCursor, DBObject, BasicDBObject, MongoClient }

import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConversions.mapAsScalaMap

package object dsl3 { outer ⇒
  import scalaz.stream.Process

  type MProcess[Out] = Process[Task, Out]
  type MStream[Out] = DBChannel[MongoClient, Out]

  object FetchMode extends Enumeration {
    type Type = Value
    val One, Batch = Value
  }

  sealed trait LogLevel
  case object ErrorLevel extends LogLevel
  case object WarnLevel extends LogLevel
  case object InfoLevel extends LogLevel
  case object DebugLevel extends LogLevel

  sealed trait Log[A]
  case class LogMsg(level: LogLevel, msg: String) extends Log[Unit]

  case class QuerySettings(q: DBObject, sort: Option[DBObject] = None, limit: Option[Int] = None, skip: Option[Int] = None)

  object Query {
    import shapeless._
    import shapeless.CNil
    import CoyonedaShapless._
    import scalaz.Free.liftFC
    import scalaz.Free.runFC

    sealed trait StatementOp[T]
    case class EqOp(q: BasicDBObject) extends StatementOp[QuerySettings]
    case class ChainOp(q: BasicDBObject) extends StatementOp[QuerySettings]
    case class Filter(q: BasicDBObject) extends StatementOp[QuerySettings]
    //case class Skip(n: Int) extends StatementOp[QuerySettings]
    //case class Limit(n: Int) extends StatementOp[QuerySettings]

    type QueryFree[A] = scalaz.Free.FreeC[StatementOp, A]

    type QueryApp[T] = StatementOp[T] :+: Log[T] :+: CNil
    type CoyoApp[T] = Coyoneda[QueryApp, T]
    type FreeApp[T] = scalaz.Free.FreeC[QueryApp, T]

    object LogQuery {
      def debug(msg: String) = Copoyo[QueryApp](LogMsg(DebugLevel, msg))
    }

    val init = QuerySettings(new BasicDBObject)

    implicit def f2FreeM(q: mongo.EqQueryFragment): QueryFree[QuerySettings] = liftFC(EqOp(q.q))
    implicit def c2FreeM(q: mongo.ComposableQueryFragment): QueryFree[QuerySettings] = liftFC(ChainOp(q.q))
    implicit def sort2FreeM(kv: (String, mongo.Order.Value)): QueryFree[QuerySettings] = liftFC(Filter(new BasicDBObject(kv._1, kv._2.id)))

    type QueryS[T] = scalaz.State[QuerySettings, T]

    object GeneralQueryInterpreter extends (Query.StatementOp ~> QueryS) {
      def apply[T](op: Query.StatementOp[T]): QueryS[T] = op match {
        case EqOp(q) ⇒
          scalaz.State { (in: QuerySettings) ⇒
            (in.copy(q = new BasicDBObject(mapAsJavaMap(mapAsScalaMap(in.q.toMap) ++ mapAsScalaMap(q.toMap)))), in)
          }
        case ChainOp(q) ⇒
          scalaz.State { (in: QuerySettings) ⇒
            (in.copy(q = new BasicDBObject(mapAsJavaMap(mapAsScalaMap(in.q.toMap) ++ mapAsScalaMap(q.toMap)))), in)
          }
        case Filter(q) ⇒ scalaz.State { (in: QuerySettings) ⇒ (in.copy(sort = Option(q)), in) }
      }
    }

    def query(rq: QueryFree[QuerySettings]): FreeApp[QuerySettings] =
      for {
        _ ← LogQuery.debug(s"Incoming query")
        q = (runFC[StatementOp, QueryS, QuerySettings](rq)(GeneralQueryInterpreter)).run(init)._1
        _ ← LogQuery.debug(s"Query: ${q.toString}")
      } yield q
  }

  object Interaction {
    import Free._
    import shapeless._
    import shapeless.CNil
    import ops.coproduct.Inject
    import CoyonedaShapless._
    import java.util.concurrent.ExecutorService
    import scalaz.stream._

    sealed trait DBError
    case object NotFound extends DBError
    case class ReadError(msg: String) extends DBError

    type NonEmptyResult = DBError \/ DBObject

    sealed trait MongoInteractionOp[T]
    case class ReadOne(client: MongoClient, qs: QuerySettings, db: String, coll: String) extends MongoInteractionOp[NonEmptyResult]
    case class ReadBatch(client: MongoClient, qs: QuerySettings, db: String, coll: String) extends MongoInteractionOp[NonEmptyResult]

    type InteractionApp[T] = MongoInteractionOp[T] :+: Log[T] :+: Query.FreeApp[T] :+: CNil
    type CoyoApp[T] = Coyoneda[InteractionApp, T]
    type FreeApp[T] = scalaz.Free.FreeC[InteractionApp, T]

    val BatchPrefix = "_id"

    def lift[F[_], A](a: F[A])(implicit inj: Inject[InteractionApp[A], F[A]]): FreeApp[A] = Copoyo[InteractionApp](a)

    def readOne(client: MongoClient, q: QuerySettings, db: String, coll: String): FreeApp[NonEmptyResult] =
      lift(ReadOne(client, q, db, coll))

    def readBatch(client: MongoClient, q: QuerySettings, db: String, coll: String): FreeApp[NonEmptyResult] =
      lift(ReadBatch(client, q, db, coll))

    def createQuery(rq: Query.QueryFree[QuerySettings]): FreeApp[QuerySettings] =
      lift(Query.query(rq))

    object LogInteraction {
      def debug(msg: String) = Copoyo[InteractionApp](LogMsg(DebugLevel, msg))
    }

    def program(rq: Query.QueryFree[QuerySettings], client: MongoClient,
                db: String, coll: String, mode: FetchMode.Type): FreeApp[NonEmptyResult] =
      for {
        qs ← createQuery(rq)
        r ← if (mode == FetchMode.One) readOne(client, qs, db, coll)
        else readBatch(client, qs, db, coll)
        _ ← LogInteraction.debug(s"Fetched result: ${r}")
      } yield (r)

    object ApacheLog4jTrans extends (Log ~> Id) {
      val logger = org.apache.log4j.Logger.getLogger("mongo-query")

      def apply[A](a: Log[A]) = a match {
        case LogMsg(lvl, msg) ⇒ logger.debug(s"$lvl: $msg")
      }
    }

    object QueryTrans extends (Query.StatementOp ~> Id) {
      override def apply[A](fa: Query.StatementOp[A]): Id[A] = fa match {
        case other ⇒ throw new Exception("This is Davy John locker")
      }
    }

    object Trampolined extends (Id ~> Trampoline) {
      def apply[A](a: A): Trampoline[A] = Trampoline.done(a)
    }

    object BatchTrans extends (MongoInteractionOp ~> Id) {
      @tailrec def go[A](cursor: DBCursor, list: List[A]): List[A] =
        if (cursor.hasNext) {
          val r = cursor.next.asInstanceOf[A]
          go(cursor, r :: list)
        } else list

      override def apply[A](fa: MongoInteractionOp[A]): Id[A] = fa match {
        case f @ ReadOne(client, qs, db, coll) ⇒
          val r = client.getDB(db).getCollection(coll).findOne(qs.q)
          if (r == null) -\/(NotFound) else \/-(r)
        case f @ ReadBatch(client, qs, db, coll) ⇒ {
          val c = client.getDB(db).getCollection(coll)
          val cursor = c.find(qs.q)
          qs.sort.foreach(cursor.sort(_))
          qs.limit.foreach(cursor.limit(_))
          qs.skip.foreach(cursor.skip(_))
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

    val qInterpreter: Query.QueryApp ~> Id = QueryTrans ||: ApacheLog4jTrans
    val qInterpreterCoyo: Query.CoyoApp ~> Id = liftCoyoLeft(qInterpreter)
    val qInterpreterFree: Query.FreeApp ~> Id = liftFree(qInterpreterCoyo)

    val intInterpreter: Interaction.InteractionApp ~> Id = BatchTrans ||: ApacheLog4jTrans ||: qInterpreterFree
    val intInterpreterCoyo: Interaction.CoyoApp ~> Id = liftCoyoLeft(intInterpreter)

    trait Streamer[M[_]] {
      protected val logger = org.apache.log4j.Logger.getLogger("mongo-query")
      def create[T](q: QuerySettings, client: MongoClient, db: String, coll: String)(implicit pool: ExecutorService): M[T]
    }

    object Streamer {
      private def mongoR[T](q: DBObject, client: MongoClient, db: String, coll: String)(implicit pool: ExecutorService): Process[Task, T] = {
        io.resource(Task.delay(client.getDB(db).getCollection(coll).find(q)))(c ⇒ Task.delay(c.close)) { c ⇒
          Task {
            if (c.hasNext) {
              val r = c.next
              r.asInstanceOf[T]
            } else throw Cause.Terminated(Cause.End)
          }
        }
      }

      implicit object ProcStreamer extends Streamer[MProcess] {
        override def create[T](q: QuerySettings, client: MongoClient, db: String, coll: String)(implicit pool: ExecutorService): MProcess[T] =
          mongoR[T](q.q, client, db, coll)
      }

      implicit val M = new scalaz.Monad[MStream]() {
        override def point[T](a: ⇒ T): MStream[T] =
          DBChannel(Process.eval(Task.now { client: MongoClient ⇒
            Task(Process.eval(Task.delay(a)))
          }))

        override def bind[T, B](fa: MStream[T])(f: (T) ⇒ MStream[B]) = fa flatMap f
      }

      implicit object MongoStreamer extends Streamer[MStream] {
        override def create[T](q: QuerySettings, client: MongoClient /*null*/ , db: String, coll: String)(implicit pool: ExecutorService): MStream[T] = {
          DBChannel(Process.eval(Task.now { client: MongoClient ⇒ Task(mongoR[T](q.q, client, db, coll)) }))
        }
      }
    }
  }

  implicit class ProgramSyntax(val self: Query.QueryFree[QuerySettings]) extends AnyVal {
    import outer.Interaction._
    import scalaz.Monad
    import scalaz.Free.runFC

    def findOne(client: MongoClient, db: String, coll: String)(implicit pool: ExecutorService) =
      Task(program(self, client, db, coll, FetchMode.One)
        .foldMap(Trampolined compose intInterpreterCoyo).run)(pool)

    def list(client: MongoClient, db: String, coll: String)(implicit pool: ExecutorService): Task[NonEmptyResult] =
      Task(program(self, client, db, coll, FetchMode.Batch)
        .foldMap(Trampolined compose intInterpreterCoyo).run)(pool)

    /**
     *
     * @param db
     * @param coll
     * @param f
     * @param pool
     * @param client
     * @tparam M
     * @return
     */
    def stream[M[_]: Monad](db: String, coll: String)(implicit f: Streamer[M], pool: ExecutorService, client: MongoClient): M[BasicDBObject] =
      f.create((runFC[Query.StatementOp, QueryS, QuerySettings](self)(Query.GeneralQueryInterpreter)).run(outer.Query.init)._1,
        client, db, coll)

    /**
     *
     * @param db
     * @param coll
     * @param f
     * @param pool
     * @param client
     * @tparam M
     * @return
     */
    def streamC[M[_]: Monad](db: String, coll: String)(implicit f: Streamer[M], pool: ExecutorService, client: MongoClient): M[BasicDBObject] = {
      f.create((runFC[Query.StatementOp, QueryS, QuerySettings](self)(Query.GeneralQueryInterpreter)).run(outer.Query.init)._1,
        client, db, coll)
    }
  }
}