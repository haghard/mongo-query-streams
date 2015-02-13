package mongo.query

import com.mongodb.DBObject
import mongo.dsl.QueryContainer
import mongo.parser.MqlParser

trait MutableQueryBuilder {

  type QuerySetting = (DBObject, String, Option[DBObject], Option[Int], Option[Int], Option[Long])

  private var query: Option[DBObject] = None
  private var sortQuery: Option[DBObject] = None
  private var collectionName: Option[String] = None

  private var limit: Option[Int] = None
  private var skip: Option[Int] = None
  private var maxTimeMS: Option[Long] = None

  private val parser = MqlParser()

  protected def qFromLine(q: String) {
    query = Some(parser.parse(q))
  }

  protected def qFromOps(ops: QueryContainer) = {
    query = Some(ops.q)
  }

  protected def sortFromLine(q: String) {
    sortQuery = Some(parser.parse(q))
  }

  protected def sortFromObj(query: QueryContainer) {
    sortQuery = Some(query.q)
  }

  protected def limit0(n: Int) {
    limit = Some(n)
  }

  protected def skip0(n: Int) {
    skip = Some(n)
  }

  protected def maxTimeMS0(mills: Long) {
    maxTimeMS = Some(mills)
  }

  protected def collection0(name: String) {
    collectionName = Some(name)
  }

  /**
   *
   * @param q
   */
  def q(q: String): Unit

  /**
   *
   * @param query
   */
  def q(query: QueryContainer): Unit

  /**
   *
   * @param q
   */
  def sort(q: String): Unit

  /**
   *
   * @param query
   */
  def sort(query: QueryContainer): Unit

  /**
   *
   * @param n
   */
  def limit(n: Int): Unit

  /**
   *
   * @param n
   */
  def skip(n: Int): Unit

  /**
   *
   * @param mills
   */
  def maxTimeMS(mills: Long): Unit

  /**
   *
   * @param name
   */
  def collection(name: String): Unit

  /**
   *
   *
   * @return
   */
  def build(): Option[QuerySetting] =
    for { q ← query; c ← collectionName } yield {
      (q, c, sortQuery, limit, skip, maxTimeMS)
    }
}