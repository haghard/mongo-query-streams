package mongo

import java.util
import com.mongodb.{ BasicDBObject, DBObject }
import scala.collection.JavaConversions._
import scalaz.syntax.Ops

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
}