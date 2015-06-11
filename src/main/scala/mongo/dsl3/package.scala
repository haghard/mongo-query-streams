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

import com.mongodb.{ DBCursor, DBObject, BasicDBObject, MongoClient }
import mongo.dsl3.Interaction.NonEmptyResult
import rx.lang.scala.Producer
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.util.Try
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

    val init = new BasicDBObject

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
    import java.util.concurrent.ExecutorService
    import scalaz.stream._
    import scalaz.stream.Process
    import rx.lang.scala.Observable

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
    type ScalazP[Out] = Process[Task, Out]

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

    val qInterpreter: Query.QueryApp ~> Id = QueryTrans ||: ApacheLog4jTrans
    val qInterpreterCoyo: Query.CoyoApp ~> Id = liftCoyoLeft(qInterpreter)
    val qInterpreterFree: Query.FreeApp ~> Id = liftFree(qInterpreterCoyo)

    val intInterpreter: Interaction.InteractionApp ~> Id = BatchTrans ||: ApacheLog4jTrans ||: qInterpreterFree
    val intInterpreterCoyo: Interaction.CoyoApp ~> Id = liftCoyoLeft(intInterpreter)

    trait StreamerFactory[M[_]] {
      protected val logger = org.apache.log4j.Logger.getLogger("mongo-query")
      def create[T](q: BasicDBObject, client: MongoClient, db: String, coll: String)(implicit pool: ExecutorService): M[T]
    }

    implicit val M = new scalaz.Monad[Observable]() {
      override def point[A](a: ⇒ A): Observable[A] = Observable.just(a)
      override def bind[A, B](fa: Observable[A])(f: (A) ⇒ Observable[B]): Observable[B] = fa flatMap f
    }

    object StreamerFactory {
      implicit object ProcStreamer extends StreamerFactory[ScalazP] {
        override def create[T](q: BasicDBObject, client: MongoClient, db: String, coll: String)(implicit pool: ExecutorService): ScalazP[T] = {
          io.resource(Task(client.getDB(db).getCollection(coll).find(q)))(c ⇒ Task(c.close)) { c ⇒
            Task {
              if (c.hasNext) {
                val r = c.next
                logger.debug(s"fetch $r")
                r.asInstanceOf[T]
              } else {
                throw Cause.Terminated(Cause.End)
              }
            }
          }
        }
      }

      implicit object RxStreamer extends StreamerFactory[Observable] {
        override def create[T](q: BasicDBObject, client: MongoClient, db: String, coll: String)(implicit pool: ExecutorService): Observable[T] = {
          Observable { subscriber: rx.lang.scala.Subscriber[T] ⇒
            subscriber.setProducer(new Producer() {
              lazy val cursor: Option[DBCursor] = (Try {
                Option(client.getDB(db).getCollection(coll).find(q))
              } recover {
                case e: Throwable ⇒
                  subscriber.onError(e)
                  None
              }).get

              @tailrec def go(n: Long): Unit = {
                if (n > 0) {
                  if (cursor.forall(_.hasNext)) {
                    val r = cursor.get.next().asInstanceOf[T]
                    logger.debug(s"fetch $r")
                    subscriber.onNext(r)
                    go(n - 1)
                  } else subscriber.onCompleted()
                }
              }

              override def request(n: Long): Unit = {
                logger.debug(s"request $n")
                Task(go(n))(pool).runAsync(_.fold((ex ⇒ subscriber.onError(ex)), (_ ⇒ ())))
              }
            })
          }
        }
      }
    }
  }

  implicit class ProgramSyntax(val self: Query.QueryBuilder[BasicDBObject]) extends AnyVal {
    def one(client: MongoClient, db: String, coll: String)(implicit pool: ExecutorService) =
      Task(outer.Interaction.program(self, client, db, coll, FetchMode.One)
        .foldMap(outer.Interaction.Trampolined compose outer.Interaction.intInterpreterCoyo).run)(pool)

    def list(client: MongoClient, db: String, coll: String)(implicit pool: ExecutorService): Task[NonEmptyResult] =
      Task(outer.Interaction.program(self, client, db, coll, FetchMode.Batch)
        .foldMap(outer.Interaction.Trampolined compose outer.Interaction.intInterpreterCoyo).run)(pool)

    def stream[M[_]: scalaz.Monad](client: MongoClient, db: String, coll: String)(implicit c: outer.Interaction.StreamerFactory[M], pool: ExecutorService): M[BasicDBObject] = {
      c.create((scalaz.Free.runFC[Query.StatementOp, QueryS, BasicDBObject](self)(Query.QueryInterpreterS)).run(outer.Query.init)._1,
        client, db, coll)
    }
  }
}