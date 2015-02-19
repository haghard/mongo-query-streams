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
import scalaz.syntax.Ops
import scalaz.concurrent.Task
import scala.collection.JavaConversions._
import com.mongodb.{ BasicDBObject, DBObject }

package object dsl {

  sealed private[mongo] trait QueryBuilder {
    def q: BasicDBObject
  }

  private[dsl] trait QueryDsl extends Ops[ChainQueryFragment] {
    mixin: { def field: String; def nested: Option[BasicDBObject] } ⇒

    private def update[T](v: T, op: String) = Option(
      nested.fold(new BasicDBObject(op, v)) { prev ⇒
        prev.append(op, v)
      })

    private def update[T](v: java.lang.Iterable[T], op: String) = Option(
      nested.fold(new BasicDBObject(op, v)) { prev ⇒
        prev.append(op, v)
      })

    def $eq[T: Values](v: T) = EqQueryFragment(field, new BasicDBObject(field, v))
    def $gt[T: Values](v: T) = self.copy(field, update(v, "$gt"))
    def $gte[T: Values](v: T) = self.copy(field, update(v, "$gte"))
    def $lt[T: Values](v: T) = self.copy(field, update(v, "$lt"))
    def $lte[T: Values](v: T) = self.copy(field, update(v, "$lte"))
    def $ne[T: Values](v: T) = self.copy(field, update(v, "$ne"))
    def $in[T: Values](vs: Iterable[T]) = self.copy(field, update(asJavaIterable(vs), "$in"))
    def $all[T: Values](vs: Iterable[T]) = self.copy(field, update(asJavaIterable(vs), "$all"))
    def $nin[T: Values](vs: Iterable[T]) = self.copy(field, update(asJavaIterable(vs), "$nin"))
  }

  private[dsl] case class EqQueryFragment(val field: String, override val q: BasicDBObject) extends QueryBuilder

  private[dsl] case class ChainQueryFragment(val field: String, val nested: Option[BasicDBObject]) extends QueryDsl with QueryBuilder {
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

  implicit def f2b(f: String) = ChainQueryFragment(f, None)

  def &&(bs: QueryBuilder*) = AndQueryFragment(bs)
  def ||(bs: QueryBuilder*) = OrQueryFragment(bs)

  import com.mongodb.DBObject
  import scalaz.{ Functor, Free }

  object free {
    import scalaz.Free.liftF

    type DslFree[T] = Free[QueryAlg, T]

    private val Separator = " , "

    private[free] sealed trait QueryAlg[+A] {
      def map[B](f: A ⇒ B): QueryAlg[B]
    }

    object QueryAlg {
      implicit val functor: Functor[QueryAlg] = new Functor[QueryAlg] {
        def map[A, B](fa: QueryAlg[A])(f: A ⇒ B): QueryAlg[B] = fa map f
      }
    }

    private case class EqFragment[+A](q: DBObject, next: DBObject ⇒ A) extends QueryAlg[A] {
      override def map[B](f: (A) ⇒ B): QueryAlg[B] = copy(next = next andThen f)
    }

    private case class ChainFragment[+A](q: DBObject, next: DBObject ⇒ A) extends QueryAlg[A] {
      override def map[B](f: (A) ⇒ B): QueryAlg[B] = copy(next = next andThen f)
    }

    implicit def frag2FreeM(fragment: EqQueryFragment): DslFree[DBObject] =
      liftF(EqFragment(fragment.q, identity[DBObject]))

    implicit def chainFrag2FreeM(fragment: ChainQueryFragment): DslFree[DBObject] =
      liftF(ChainFragment(fragment.q, identity[DBObject]))

    def step[T](exp: QueryAlg[DslFree[T]]): Task[DslFree[T]] =
      exp match {
        case EqFragment(q, next)    ⇒ Task now { q } map (next)
        case ChainFragment(q, next) ⇒ Task now { q } map (next)
      }

    def instructions[T](program: DslFree[T], acts: List[String] = Nil): String =
      program.resume.fold(
        {
          case EqFragment(q, next)    ⇒ instructions(next(q), q.toString :: acts)
          case ChainFragment(q, next) ⇒ instructions(next(q), q.toString :: acts)
        }, { r: T ⇒
          if (acts.size > 1) {
            val ops = acts.reverse
            val line = ops.tail.foldLeft(new scala.StringBuilder(ops.head.dropRight(1)).append(Separator)) { (acc, c) ⇒
              acc.append(c.drop(1)).append(Separator)
            }.toString
            line dropRight 3
          } else acts.head

          /*if (acts.size > 1) {
            val line = acts.reverse.foldLeft(new scala.StringBuilder().append("""{ "$and" : [ """)) { (acc, c) ⇒
              acc.append(c).append(Separator)
            }.toString
            val clean = if (line.substring(line.length - 3, line.length) == Separator) line.dropRight(3) else line
            clean + "]}"
          } else acts.head*/
        })
  }
  // val q = NestedMap(("article" -> Seq($gt().op -> 3, $lt().op -> 90)),("producer_num" -> Seq(($eq().op -> 1))))
  // { "num" : { "$gt" : 3, "$lt" : 90 } , "name" : { "$ne" : false } }
}
