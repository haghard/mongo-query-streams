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

import java.util
import org.apache.log4j.Logger

import scalaz.{ Monad, State, ~> }
import scalaz.concurrent.Task
import scala.collection.JavaConversions._
import com.mongodb.{ BasicDBObject, DBObject }

package object dsl {

  sealed private[mongo] trait QueryBuilder {
    def q: BasicDBObject
  }

  private[dsl] trait QueryDsl extends scalaz.syntax.Ops[ComposableQuery] {

    def field: String

    def nested: Option[BasicDBObject]

    private def update[T](v: T, op: String) = Option(
      nested.fold(new BasicDBObject(op, v)) { prev ⇒
        prev.append(op, v)
      })

    private def update[T](v: java.lang.Iterable[T], op: String) = Option(
      nested.fold(new BasicDBObject(op, v)) { prev ⇒
        prev.append(op, v)
      })

    def $eq[T: Values](v: T) = EqQueryFragment(new BasicDBObject(field, v))
    def $gt[T: Values](v: T) = self.copy(field, update(v, "$gt"))
    def $gte[T: Values](v: T) = self.copy(field, update(v, "$gte"))
    def $lt[T: Values](v: T) = self.copy(field, update(v, "$lt"))
    def $lte[T: Values](v: T) = self.copy(field, update(v, "$lte"))
    def $ne[T: Values](v: T) = self.copy(field, update(v, "$ne"))
    def $in[T: Values](vs: Iterable[T]) = self.copy(field, update(asJavaIterable(vs), "$in"))
    def $all[T: Values](vs: Iterable[T]) = self.copy(field, update(asJavaIterable(vs), "$all"))
    def $nin[T: Values](vs: Iterable[T]) = self.copy(field, update(asJavaIterable(vs), "$nin"))
  }

  private[dsl] case class EqQueryFragment(override val q: BasicDBObject) extends QueryBuilder

  private[dsl] case class ComposableQuery(val field: String, val nested: Option[BasicDBObject]) extends QueryDsl with QueryBuilder {
    override val self = this
    override def q = new BasicDBObject(field, nested.fold(new BasicDBObject())(x ⇒ x))
    override def toString() = q.toString
  }

  private[dsl] case class AndQueryFragment(cs: TraversableOnce[QueryBuilder]) extends QueryBuilder {
    override def q = new BasicDBObject("$and", cs.foldLeft(new util.ArrayList[DBObject]()) { (arr, c) ⇒
      arr.add(c.q)
      arr
    })
    override def toString() = q.toString
  }

  private[dsl] case class OrQueryFragment(cs: TraversableOnce[QueryBuilder]) extends QueryBuilder {
    override def q = new BasicDBObject("$or", cs.foldLeft(new util.ArrayList[DBObject]()) { (arr, c) ⇒
      arr.add(c.q)
      arr
    })
    override def toString() = q.toString
  }

  implicit def f2b(f: String) = ComposableQuery(f, None)

  def &&(bs: QueryBuilder*) = AndQueryFragment(bs)
  def ||(bs: QueryBuilder*) = OrQueryFragment(bs)

  import com.mongodb.DBObject
  import scalaz.{ Functor, Free }

  object free {
    import scalaz.Free.liftF
    import scala.collection.JavaConversions.mapAsJavaMap
    import scala.collection.JavaConversions.mapAsScalaMap

    private val logger = Logger.getLogger("monadic-query")

    type FreeQuery[T] = Free[QueryAlg, T]
    type QueryState[T] = State[BasicDBObject, T]

    private val Separator = " , "
    private val empty = new BasicDBObject()

    private[free] sealed trait QueryAlg[+A] {
      def map[B](f: A ⇒ B): QueryAlg[B]
    }

    object QueryAlg {
      implicit val functor: Functor[QueryAlg] = new Functor[QueryAlg] {
        def map[A, B](fa: QueryAlg[A])(f: A ⇒ B): QueryAlg[B] = fa map f
      }
    }

    implicit val mongoFreeMonad: Monad[FreeQuery] = new Monad[FreeQuery] {
      def point[A](a: ⇒ A) = Free.point(a)
      def bind[A, B](fa: FreeQuery[A])(f: A ⇒ FreeQuery[B]) = fa flatMap f
    }

    private case class EqFragment[+A](q: BasicDBObject, next: DBObject ⇒ A) extends QueryAlg[A] {
      override def map[B](f: (A) ⇒ B): QueryAlg[B] = copy(next = next andThen f)
    }

    private case class ComposableFragment[+A](q: BasicDBObject, next: DBObject ⇒ A) extends QueryAlg[A] {
      override def map[B](f: (A) ⇒ B): QueryAlg[B] = copy(next = next andThen f)
    }

    implicit def frag2FreeM(fragment: EqQueryFragment): FreeQuery[DBObject] =
      liftF(EqFragment(fragment.q, identity[DBObject]))

    implicit def chainFrag2FreeM(fragment: ComposableQuery): FreeQuery[DBObject] =
      liftF(ComposableFragment(fragment.q, identity[DBObject]))

    //
    //program.runM(step)
    def step[T](op: QueryAlg[FreeQuery[T]]): Task[FreeQuery[T]] =
      op match {
        case EqFragment(q, next)    ⇒ Task now { logger.debug(q); q } map (next)
        case ComposableFragment(q, next) ⇒ Task now { logger.debug(q); q } map (next)
      }

    /**
     * Natural transformations, map one functor QueryAlg to QueryState
     */
    private def runState: QueryAlg ~> QueryState = new (QueryAlg ~> QueryState) {
      def apply[T](op: QueryAlg[T]): QueryState[T] = op match {
        case EqFragment(q, next) ⇒
          State { (ops: BasicDBObject) ⇒
            val m = mapAsJavaMap(mapAsScalaMap(ops.toMap) ++ mapAsScalaMap(q.toMap))
            (new BasicDBObject(m), next(q))
          }
        case ComposableFragment(q, next) ⇒
          State { (ops: BasicDBObject) ⇒
            val m = mapAsJavaMap(mapAsScalaMap(ops.toMap) ++ mapAsScalaMap(q.toMap))
            (new BasicDBObject(m), next(q))
          }
      }
    }

    implicit class ProgramImplicits[T](val program: FreeQuery[T]) extends AnyVal {
      def toDBObject: DBObject = program foldMap (runState) exec (empty)
      def toQuery: String = loop(program, Nil)
      private def loop(program: FreeQuery[T], acts: List[String] = Nil): String =
        program.resume.fold(
          {
            case EqFragment(q, next)    ⇒ loop(next(q), q.toString :: acts)
            case ComposableFragment(q, next) ⇒ loop(next(q), q.toString :: acts)
          }, { r: T ⇒
            if (acts.size > 1) {
              val ops = acts.reverse
              val line = ops.tail.foldLeft(new scala.StringBuilder(ops.head.dropRight(1)).append(Separator)) { (acc, c) ⇒
                acc.append(c.drop(1)).append(Separator)
              }.toString
              line dropRight 3
            } else acts.head
          })
    }
  }
}
