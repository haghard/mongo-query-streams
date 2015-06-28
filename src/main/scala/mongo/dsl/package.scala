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

import cassandra.{ CassandraParamValue, CassandraQuerySettings }

import scala.reflect.ClassTag
import scala.runtime.ScalaRunTime
import scala.util.Try
import mongo.query.DBChannel
import scalaz.concurrent.Task
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext
import java.util.concurrent.ExecutorService
import rx.lang.scala.schedulers.ExecutionContextScheduler
import rx.lang.scala.{ Producer, Subscriber, Observable }
import scalaz.{ Free, Coyoneda, \/, -\/, \/-, Trampoline, ~> }
import com.mongodb.{ DBObject, DBCursor, BasicDBObject, MongoClient }

import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConversions.mapAsScalaMap

package object dsl { outer ⇒
  import scalaz.stream.Process

  type MProcess[Out] = Process[Task, Out]
  type MStream[Out] = DBChannel[MongoClient, Out]

  sealed trait LogLevel
  case object ErrorLevel extends LogLevel
  case object WarnLevel extends LogLevel
  case object InfoLevel extends LogLevel
  case object DebugLevel extends LogLevel

  sealed trait Log[A]
  case class LogMsg(level: LogLevel, msg: String) extends Log[Unit]

  case class MongoQuerySettings(q: DBObject, sort: Option[DBObject] = None,
                                limit: Option[Int] = None, skip: Option[Int] = None)

  class MyTag[A](val t: ClassTag[A]) extends ClassTag[A] {
    override def runtimeClass = t.runtimeClass
    override def unapply(x: Any): Option[A] = {
      if (t.runtimeClass.isPrimitive) {
        val ok = x match {
          case _: java.lang.Integer   ⇒ runtimeClass == java.lang.Integer.TYPE
          //case _: java.lang.Double    => runtimeClass == java.lang.Double.TYPE
          case _: java.lang.Double    ⇒ t == ClassTag.Double // equivalent
          case _: java.lang.Long      ⇒ runtimeClass == java.lang.Long.TYPE
          case _: java.lang.Character ⇒ runtimeClass == java.lang.Character.TYPE
          case _: java.lang.Float     ⇒ runtimeClass == java.lang.Float.TYPE
          case _: java.lang.Byte      ⇒ runtimeClass == java.lang.Byte.TYPE
          case _: java.lang.Short     ⇒ runtimeClass == java.lang.Short.TYPE
          case _: java.lang.Boolean   ⇒ runtimeClass == java.lang.Boolean.TYPE
          case _: Unit                ⇒ runtimeClass == java.lang.Void.TYPE
          case _                      ⇒ false // super.unapply(x).isDefined
        }
        if (ok) Some(x.asInstanceOf[A]) else None
      } else if (x == null) {
        // let them collect nulls, for example
        if (t == ClassTag.Null) Some(null.asInstanceOf[A]) else None
      } else super.unapply(x)
    }
  }

  object qb {
    import shapeless._
    import shapeless.CNil
    import CoyonedaShapless._
    import scalaz.Free.liftFC
    import scalaz.Free.runFC

    sealed trait StatementOp[T]
    case class EqOp(q: DBObject) extends StatementOp[MongoQuerySettings]
    case class ChainOp(q: DBObject) extends StatementOp[MongoQuerySettings]
    case class Sort(q: DBObject) extends StatementOp[MongoQuerySettings]
    case class Skip(n: Int) extends StatementOp[MongoQuerySettings]
    case class Limit(n: Int) extends StatementOp[MongoQuerySettings]
    case class CassandraSelect(q: String) extends StatementOp[cassandra.CassandraQuerySettings]
    case class CassandraParam(name: String, v: AnyRef, c: Class[_]) extends StatementOp[cassandra.CassandraQuerySettings]

    type QueryFree[A] = scalaz.Free.FreeC[StatementOp, A]

    type QueryApp[T] = StatementOp[T] :+: Log[T] :+: CNil
    type CoyoApp[T] = Coyoneda[QueryApp, T]
    type FreeApp[T] = scalaz.Free.FreeC[QueryApp, T]

    object LogQuery {
      def debug(msg: String) = Copoyo[QueryApp](LogMsg(DebugLevel, msg))
    }

    val init = MongoQuerySettings(new BasicDBObject)

    implicit def f2FreeM(q: mongo.EqQueryFragment): QueryFree[MongoQuerySettings] = liftFC(EqOp(q.q))
    implicit def c2FreeM(q: mongo.ComposableQueryFragment): QueryFree[MongoQuerySettings] = liftFC(ChainOp(q.q))
    implicit def sort2FreeM(kv: (String, mongo.Order.Value)): QueryFree[MongoQuerySettings] = liftFC(Sort(new BasicDBObject(kv._1, kv._2.id)))

    implicit def qFreeM(select: String): QueryFree[cassandra.CassandraQuerySettings] = liftFC(CassandraSelect(select))

    /**
     * @tparam T something from java.lang. for correct work with cassandra driver
     */
    def cParam[T <: AnyRef](name: String, v: T)(implicit t: ClassTag[T]): QueryFree[cassandra.CassandraQuerySettings] =
      liftFC(CassandraParam(name, v, t.runtimeClass))

    def sort(h: (String, Order.Value), t: (String, Order.Value)*): QueryFree[MongoQuerySettings] = {
      liftFC(Sort(t.toList.foldLeft(new BasicDBObject(h._1, h._2.id)) { (acc, c) ⇒
        acc.append(c._1, c._2.id)
      }))
    }

    def skip(n: Int): QueryFree[MongoQuerySettings] = liftFC(Skip(n))
    def limit(n: Int): QueryFree[MongoQuerySettings] = liftFC(Limit(n))

    type QueryM[T] = scalaz.State[MongoQuerySettings, T]
    type QueryC[T] = scalaz.State[CassandraQuerySettings, T]

    object CassandraQueryInterpreter extends (qb.StatementOp ~> QueryC) {
      override def apply[A](fa: qb.StatementOp[A]): QueryC[A] = fa match {
        case CassandraSelect(select) ⇒
          scalaz.State { (in: CassandraQuerySettings) ⇒ (in.copy(select), in) }
        case CassandraParam(name, v, c) ⇒
          scalaz.State { (in: CassandraQuerySettings) ⇒ (in.copy(v = Some(CassandraParamValue(name, v, c))), in) }
      }
    }

    object MongoQueryInterpreter extends (qb.StatementOp ~> QueryM) {
      def apply[T](op: qb.StatementOp[T]): QueryM[T] = op match {
        case EqOp(q) ⇒
          scalaz.State { (in: MongoQuerySettings) ⇒
            (in.copy(q = new BasicDBObject(mapAsJavaMap(mapAsScalaMap(in.q.toMap) ++ mapAsScalaMap(q.toMap)))), in)
          }
        case ChainOp(q) ⇒
          scalaz.State { (in: MongoQuerySettings) ⇒
            (in.copy(q = new BasicDBObject(mapAsJavaMap(mapAsScalaMap(in.q.toMap) ++ mapAsScalaMap(q.toMap)))), in)
          }
        case Sort(q)  ⇒ scalaz.State { (in: MongoQuerySettings) ⇒ (in.copy(sort = Option(q)), in) }
        case Skip(n)  ⇒ scalaz.State { (in: MongoQuerySettings) ⇒ (in.copy(skip = Option(n)), in) }
        case Limit(n) ⇒ scalaz.State { (in: MongoQuerySettings) ⇒ (in.copy(limit = Option(n)), in) }
      }
    }

    def query(rq: QueryFree[MongoQuerySettings]): FreeApp[MongoQuerySettings] =
      for {
        _ ← LogQuery.debug("Construct query")
        q = (runFC[StatementOp, QueryM, MongoQuerySettings](rq)(MongoQueryInterpreter)).run(init)._1
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
    case class ReadOne(client: MongoClient, qs: MongoQuerySettings, db: String, coll: String) extends MongoInteractionOp[NonEmptyResult]
    case class ReadBatch(client: MongoClient, qs: MongoQuerySettings, db: String, coll: String) extends MongoInteractionOp[NonEmptyResult]

    type InteractionApp[T] = MongoInteractionOp[T] :+: Log[T] :+: qb.FreeApp[T] :+: CNil
    type CoyoApp[T] = Coyoneda[InteractionApp, T]
    type FreeApp[T] = scalaz.Free.FreeC[InteractionApp, T]

    val BatchPrefix = "_id"

    def lift[F[_], A](a: F[A])(implicit inj: Inject[InteractionApp[A], F[A]]): FreeApp[A] = Copoyo[InteractionApp](a)

    def readOne(client: MongoClient, q: MongoQuerySettings, db: String, coll: String): FreeApp[NonEmptyResult] =
      lift(ReadOne(client, q, db, coll))

    def readBatch(client: MongoClient, q: MongoQuerySettings, db: String, coll: String): FreeApp[NonEmptyResult] =
      lift(ReadBatch(client, q, db, coll))

    def createQuery(rq: qb.QueryFree[MongoQuerySettings]): FreeApp[MongoQuerySettings] = lift(qb.query(rq))

    object LogInteraction {
      def debug(msg: String) = Copoyo[InteractionApp](LogMsg(DebugLevel, msg))
      def info(msg: String) = Copoyo[InteractionApp](LogMsg(InfoLevel, msg))
      def warn(msg: String) = Copoyo[InteractionApp](LogMsg(WarnLevel, msg))
      def error(msg: String) = Copoyo[InteractionApp](LogMsg(ErrorLevel, msg))
    }

    def program(rq: qb.QueryFree[MongoQuerySettings], client: MongoClient,
                db: String, coll: String, mode: FetchMode.Type): FreeApp[NonEmptyResult] =
      for {
        qs ← createQuery(rq)
        _ ← LogInteraction.debug(s"Query:[ ${qs.q} ] Sort:[ ${qs.sort} ] Skip:[ ${qs.skip} ] Limit:[ ${qs.limit} ]")
        r ← if (mode == FetchMode.One) readOne(client, qs, db, coll) else readBatch(client, qs, db, coll)
        _ ← LogInteraction.debug(s"fetch: $r")
      } yield (r)

    object ApacheLog4jTransformation extends (Log ~> Id) {
      val logger = org.apache.log4j.Logger.getLogger("mongo-query")
      def apply[A](a: Log[A]) = a match {
        case LogMsg(DebugLevel, msg) ⇒ logger.debug(msg)
        case LogMsg(InfoLevel, msg)  ⇒ logger.info(msg)
        case LogMsg(WarnLevel, msg)  ⇒ logger.warn(msg)
        case LogMsg(ErrorLevel, msg) ⇒ logger.error(msg)
      }
    }

    object QueryTrans extends (qb.StatementOp ~> Id) {
      override def apply[A](fa: qb.StatementOp[A]): Id[A] = fa match {
        case other ⇒ throw new Exception("This is Davy John locker")
      }
    }

    object Trampolined extends (Id ~> Trampoline) {
      def apply[A](a: A): Trampoline[A] = Trampoline.done(a)
    }

    object BatchQueryTransformation extends (MongoInteractionOp ~> Id) {
      @tailrec def go[A](cursor: DBCursor, list: Vector[A]): Vector[A] =
        if (cursor.hasNext) {
          val r = cursor.next.asInstanceOf[A]
          go(cursor, list.:+(r))
        } else list

      override def apply[A](fa: MongoInteractionOp[A]): Id[A] = fa match {
        case f @ ReadOne(client, qs, db, coll) ⇒
          val r = client.getDB(db).getCollection(coll).findOne(qs.q)
          if (r == null) -\/(NotFound) else \/-(r)
        case f @ ReadBatch(client, qs, db, coll) ⇒ {
          val collection = client.getDB(db).getCollection(coll)
          val cursor = collection.find(qs.q)
          qs.sort.foreach(cursor.sort(_))
          qs.limit.foreach(cursor.limit(_))
          qs.skip.foreach(cursor.skip(_))
          try {
            \/-(new BasicDBObject(BatchPrefix, seqAsJavaList(go(cursor, Vector.empty))))
          } catch {
            case e: Exception ⇒ -\/(ReadError(e.getMessage))
          } finally {
            cursor.close
          }
        }
      }
    }

    val qInterpreter: qb.QueryApp ~> Id = QueryTrans ||: ApacheLog4jTransformation
    val qInterpreterCoyo: qb.CoyoApp ~> Id = liftCoyoLeft(qInterpreter)
    val qInterpreterFree: qb.FreeApp ~> Id = liftFree(qInterpreterCoyo)

    val intInterpreter: Interaction.InteractionApp ~> Id = BatchQueryTransformation ||: ApacheLog4jTransformation ||: qInterpreterFree
    val intInterpreterCoyo: Interaction.CoyoApp ~> Id = liftCoyoLeft(intInterpreter)

    trait Streamer[M[_]] {
      protected val logger = org.apache.log4j.Logger.getLogger("streamer-query")
      def create[T](q: MongoQuerySettings, client: MongoClient, db: String, coll: String)(implicit pool: ExecutorService): M[T]
    }

    trait ChannelStreamer[M[_]] {
      protected val logger = org.apache.log4j.Logger.getLogger("streamer-channel-query")
      def create[T](q: MongoQuerySettings, db: String, coll: String)(implicit pool: ExecutorService): M[T]
    }

    private[dsl] def mongoResource[T](qs: MongoQuerySettings, client: MongoClient, db: String, collection: String, log: org.apache.log4j.Logger)(implicit pool: ExecutorService): Process[Task, T] = {
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
            r.asInstanceOf[T]
          } else throw Cause.Terminated(Cause.End)
        }
      }
    }

    object ChannelStreamer {
      implicit object channelStreamerProc extends ChannelStreamer[MStream] {
        override def create[T](q: MongoQuerySettings, db: String, coll: String)(implicit pool: ExecutorService): MStream[T] =
          DBChannel(Process.eval(Task.now { client: MongoClient ⇒
            Task(mongoResource[T](q, client, db, coll, logger))
          }))
      }
    }

    object Streamer {
      implicit object ProcStreamer extends Streamer[MProcess] {
        override def create[T](q: MongoQuerySettings, client: MongoClient, db: String, coll: String)(implicit pool: ExecutorService): MProcess[T] =
          mongoResource[T](q, client, db, coll, logger)
      }

      implicit object RxStreamer extends Streamer[Observable] {
        import com.mongodb.{ MongoClient, DBCursor }
        override def create[T](qs: MongoQuerySettings, client: MongoClient, db: String, collection: String)(implicit pool: ExecutorService): Observable[T] = {
          Observable { subscriber: Subscriber[T] ⇒
            subscriber.setProducer(new Producer() {
              lazy val cursor: Option[DBCursor] = (Try {
                Option {
                  val coll = client.getDB(db).getCollection(collection)
                  val cursor = coll.find(qs.q)
                  qs.sort.foreach(cursor.sort(_))
                  qs.skip.foreach(cursor.skip(_))
                  qs.limit.foreach(cursor.limit(_))
                  logger.debug(s"Query-settings: Sort:[ ${qs.sort} ] Skip:[ ${qs.skip} ] Limit:[ ${qs.limit} ] Query:[ ${qs.q} ]")
                  cursor
                }
              } recover {
                case e: Throwable ⇒
                  subscriber.onError(e)
                  None
              }).get

              @tailrec def go(n: Long): Unit = {
                logger.info(s"request $n")
                if (n > 0) {
                  if (cursor.find(_.hasNext).isDefined) {
                    val r = cursor.get.next().asInstanceOf[T]
                    logger.info(s"fetch $r")
                    subscriber.onNext(r)
                    go(n - 1)
                  } else subscriber.onCompleted
                }
              }

              override def request(n: Long): Unit = go(n)
            })
          }.subscribeOn(ExecutionContextScheduler(ExecutionContext.fromExecutor(pool)))
        }
      }
    }
  }

  implicit class ProgramSyntax(val self: qb.QueryFree[MongoQuerySettings]) extends AnyVal {
    import outer.Interaction._
    import scalaz.Free.runFC
    import mongo.dsl.qb._

    private def init = MongoQuerySettings(new BasicDBObject)

    /**
     *
     * @return DBObject
     */
    def toDBObject = ((runFC[StatementOp, QueryM, MongoQuerySettings](self)(MongoQueryInterpreter)).run(init)._1).q

    /**
     *
     * @return String
     */
    def toQuery = toDBObject.toString

    /**
     *
     * @param client
     * @param db
     * @param coll
     * @param pool
     * @return
     */
    def findOne(client: MongoClient, db: String, coll: String)(implicit pool: ExecutorService) =
      Task(program(self, client, db, coll, FetchMode.One)
        .foldMap(Trampolined compose intInterpreterCoyo).run)(pool)

    /**
     *
     * @param client
     * @param db
     * @param coll
     * @param pool
     * @return
     */
    def list(client: MongoClient, db: String, coll: String)(implicit pool: ExecutorService): Task[NonEmptyResult] =
      Task(program(self, client, db, coll, FetchMode.Batch)
        .foldMap(Trampolined compose intInterpreterCoyo).run)(pool)

    /**
     * Works only with [[mongo.dsl.MStream]] type, allows you to do joins for processes
     *
     *
     * @param db
     * @param coll
     * @param pool
     * @tparam M
     * @return
     */
    def sChannel[M[_]: ChannelStreamer](db: String, coll: String)(implicit pool: ExecutorService): M[DBObject] =
      implicitly[ChannelStreamer[M]].create(
        runFC[qb.StatementOp, QueryM, MongoQuerySettings](self)(qb.MongoQueryInterpreter).run(outer.qb.init)._1, db, coll)

    /**
     * Works with [[mongo.dsl.MProcess]], [[rx.lang.scala.Observable]] types
     * @param db
     * @param coll
     * @param pool
     * @param client
     * @tparam M
     * @return
     */
    //M[_]: scalaz.Monad
    //val m = implicitly[scalaz.Monad[M]]
    def stream[M[_]: Streamer](db: String, coll: String)(implicit pool: ExecutorService, client: MongoClient): M[DBObject] = {
      implicitly[Streamer[M]].create(
        (runFC[qb.StatementOp, QueryM, MongoQuerySettings](self)(qb.MongoQueryInterpreter)).run(outer.qb.init)._1, client, db, coll)
    }
  }
}