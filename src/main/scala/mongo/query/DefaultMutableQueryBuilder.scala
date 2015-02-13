package mongo.query

import mongo.dsl.QueryContainer

final class DefaultMutableQueryBuilder extends MutableQueryBuilder {

  override def limit(n: Int): Unit = limit0(n)

  override def skip(n: Int): Unit = skip0(n)

  override def maxTimeMS(mills: Long): Unit = maxTimeMS0(mills)

  override def collection(name: String): Unit = collection0(name)

  override def sort(q: String) = sortFromLine(q)

  override def sort(query: QueryContainer) = sortFromObj(query)

  override def q(q: String): Unit = qFromLine(q)

  override def q(ops: QueryContainer): Unit = qFromOps(ops)
}
