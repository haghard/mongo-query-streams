package mongo.dsl

import java.util

import com.mongodb.{ BasicDBObject, DBObject }
import mongo.Values

import scala.collection.JavaConversions._

sealed trait QueryContainer {
  def q: BasicDBObject
}

trait QueryDsl { self ⇒

  private def single[T](field: String, v: T, q: Option[BasicDBObject]) =
    LeafBuilder(new BasicDBObject(field, v))

  private def chain[T](field: String, q: Option[BasicDBObject], op: String, v: T) = {
    q.fold(new ChainBuilder(field, new BasicDBObject(op, v))) { nested ⇒
      new ChainBuilder(field, nested.append(op, v))
    }
  }

  private def chain[T](field: String, q: Option[BasicDBObject], op: String, vs: java.lang.Iterable[T]) = {
    q.fold(new ChainBuilder(field, new BasicDBObject(op, vs))) { nested ⇒
      new ChainBuilder(field, nested.append(op, vs))
    }
  }

  private[QueryDsl] class BuilderOps(field: String, q: Option[BasicDBObject]) {
    def $eq[T: Values](v: T) = self.single(field, v, q)
    def $ne[T: Values](v: T) = self.chain(field, q, "$ne", v)
    def $gt[T: Values](v: T) = self.chain(field, q, "$gt", v)
    def $gte[T: Values](v: T) = self.chain(field, q, "$gte", v)
    def $lt[T: Values](v: T) = self.chain(field, q, "$lt", v)
    def $lte[T: Values](v: T) = self.chain(field, q, "$lte", v)
    def $in[T: Values](vs: Iterable[T]) = self.chain(field, q, "$in", asJavaIterable(vs))
    def $all[T: Values](vs: Iterable[T]) = self.chain(field, q, "$all", asJavaIterable(vs))
    def $nin[T: Values](vs: Iterable[T]) = self.chain(field, q, "$nin", asJavaIterable(vs))
  }

  final case class ChainBuilder(field: String, nested: BasicDBObject)
      extends BuilderOps(field, Some(nested)) with QueryContainer {
    override lazy val q = new BasicDBObject(field, nested)
  }

  private[QueryDsl] final case class AndBuilder(private val cs: TraversableOnce[QueryContainer]) extends QueryContainer {
    override lazy val q = new BasicDBObject("$and", cs.foldLeft(new util.ArrayList[DBObject]()) { (arr, c) ⇒
      arr.add(c.q)
      arr
    })
  }

  private[QueryDsl] final case class OrBuilder(private val cs: TraversableOnce[QueryContainer]) extends QueryContainer {
    override lazy val q = new BasicDBObject("$or", cs.foldLeft(new util.ArrayList[DBObject]()) { (arr, c) ⇒
      arr.add(c.q)
      arr
    })
  }

  final case class LeafBuilder(override val q: BasicDBObject) extends QueryContainer
}

object QueryDsl extends QueryDsl {

  implicit def f2b(f: String) = new BuilderOps(f, None)

  def &&(bs: QueryContainer*) = AndBuilder(bs)
  def ||(bs: QueryContainer*) = OrBuilder(bs)
}