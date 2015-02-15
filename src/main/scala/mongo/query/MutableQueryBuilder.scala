package mongo.query

import com.mongodb.DBObject
import mongo.dsl.QueryContainer
import mongo.parser.MqlParser

import scala.util.{ Failure, Success, Try }
import scalaz.{ -\/, \/-, \/, Scalaz }

trait MutableQueryBuilder {

  case class QuerySetting(q: DBObject, collName: String, sortQuery: Option[DBObject],
                          limit: Option[Int], skip: Option[Int], maxTimeMS: Option[Long])

  private var query: String \/ Option[DBObject] = \/-(None)
  private var sortQuery: String \/ Option[DBObject] = \/-(None)

  private var collectionName: Option[String] = None

  private var limit: Option[Int] = None
  private var skip: Option[Int] = None
  private var maxTimeMS: Option[Long] = None

  private val parser = MqlParser()

  private def parse0(query: String): String \/ Option[DBObject] = {
    Try(parser.parse(query)) match {
      case Success(q)  ⇒ (\/-(Option(q)))
      case Failure(er) ⇒ -\/(er.getMessage)
    }
  }

  protected def qFromLine(q: String) {
    query = parse0(q)
  }

  protected def qFromOps(ops: QueryContainer) = {
    query = \/-(Option(ops.q))
  }

  protected def sortFromLine(q: String) {
    sortQuery = parse0(q)
  }

  protected def sortFromObj(query: QueryContainer) {
    sortQuery = \/-(Option(query.q))
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
  import Scalaz._
  def build(): String \/ QuerySetting =
    for {
      qOr ← query
      q ← qOr \/> "query shouldn't be empty"
      c ← collectionName \/> "collectionName shouldn't be empty"
      s ← sortQuery
    } yield {
      QuerySetting(q, c, s, limit, skip, maxTimeMS)
    }
}