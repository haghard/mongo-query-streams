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

import com.mongodb.BasicDBObject
import org.apache.log4j.Logger

import scalaz.concurrent.Task
import scalaz.~>
import com.mongodb.DBObject

import scala.collection.JavaConversions.{ mapAsJavaMap, mapAsScalaMap }
import scalaz.Free.liftF
import scalaz.{ Free, Functor }

package object dsl {
  private val logger = Logger.getLogger("dsl")

  type FreeQuery[T] = Free[QueryAlg, T]
  type QueryState[T] = scalaz.State[BasicDBObject, T]

  private val Separator = " , "
  private val empty = new BasicDBObject()

  private[dsl] sealed trait QueryAlg[+A] {
    def map[B](f: A ⇒ B): QueryAlg[B]
  }

  object QueryAlg {
    implicit val functor: Functor[QueryAlg] = new Functor[QueryAlg] {
      def map[A, B](fa: QueryAlg[A])(f: A ⇒ B): QueryAlg[B] = fa map f
    }
  }

  implicit val mongoFreeMonad: scalaz.Monad[FreeQuery] = new scalaz.Monad[FreeQuery] {
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

  implicit def chainFrag2FreeM(fragment: ComposableQueryFragment): FreeQuery[DBObject] =
    liftF(ComposableFragment(fragment.q, identity[DBObject]))

  //
  //program.runM(step)
  def step[T](op: QueryAlg[FreeQuery[T]]): Task[FreeQuery[T]] =
    op match {
      case EqFragment(q, next) ⇒ Task now {
        logger.debug(q); q
      } map (next)
      case ComposableFragment(q, next) ⇒ Task now {
        logger.debug(q); q
      } map (next)
    }

  /**
   * Natural transformations, map one functor QueryAlg to QueryState
   */
  private def runState: QueryAlg ~> QueryState = new (QueryAlg ~> QueryState) {
    def apply[T](op: QueryAlg[T]): QueryState[T] = op match {
      case EqFragment(q, next) ⇒
        scalaz.State { (ops: BasicDBObject) ⇒
          val m = mapAsJavaMap(mapAsScalaMap(ops.toMap) ++ mapAsScalaMap(q.toMap))
          (new BasicDBObject(m), next(q))
        }
      case ComposableFragment(q, next) ⇒
        scalaz.State { (ops: BasicDBObject) ⇒
          val m = mapAsJavaMap(mapAsScalaMap(ops.toMap) ++ mapAsScalaMap(q.toMap))
          (new BasicDBObject(m), next(q))
        }
    }
  }

  implicit class ProgramSyntax[T](val program: FreeQuery[T]) extends AnyVal {

    def toDBObject: DBObject = program foldMap (runState) exec (empty)

    def toQuery: String = loop(program, Nil)

    private def loop(program: FreeQuery[T], acts: List[String] = Nil): String =
      program.resume.fold(
        {
          case EqFragment(q, next)         ⇒ loop(next(q), q.toString :: acts)
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
