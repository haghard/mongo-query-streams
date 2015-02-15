package mongo.query

import java.util.concurrent.{ TimeUnit, ExecutorService }

import com.mongodb._
import mongo._
import mongo.parser.MqlParser
import mongo.dsl.QueryContainer
import org.apache.log4j.Logger

import scalaz.concurrent.Task
import scalaz.stream.Process._
import scalaz.stream._
import scalaz.{ -\/, \/-, \/, Scalaz }
import scala.util.{ Failure, Success, Try }

private[query] trait MutableBuilder {

  private[query] var skip: Option[Int] = None
  private[query] var limit: Option[Int] = None
  private[query] var maxTimeMS: Option[Long] = None
  private[query] var collectionName: Option[String] = None
  private[query] var query: String \/ Option[DBObject] = \/-(None)
  private[query] var dbName: Option[String] = None
  private[query] var sortQuery: String \/ Option[DBObject] = \/-(None)

  private val parser = MqlParser()

  private def parse(query: String): String \/ Option[DBObject] = {
    Try(parser.parse(query)) match {
      case Success(q)  ⇒ (\/-(Option(q)))
      case Failure(er) ⇒ -\/(er.getMessage)
    }
  }

  def q(q: String): Unit = query = parse(q)

  def q(qc: QueryContainer): Unit = query = \/-(Option(qc.q))

  def db(name: String): Unit = dbName = Option(name)

  def sort(q: String): Unit = sortQuery = parse(q)

  def sort(query: QueryContainer): Unit = sortQuery = \/-(Option(query.q))

  def limit(n: Int): Unit = limit = Some(n)

  def skip(n: Int): Unit = skip = Some(n)

  def maxTimeMS(mills: Long): Unit = maxTimeMS = Some(mills)

  def collection(name: String): Unit = collectionName = Some(name)

  def build(): String \/ QuerySetting
}

object Query {
  import Scalaz._
  import mongo.MongoQuery

  def query[T](f: MutableBuilder ⇒ Unit)(implicit q: MongoQuery[T], pool: ExecutorService): MongoProcess[T, DBObject] = {
    val builder = new MutableBuilder {
      override def build(): String \/ QuerySetting =
        for {
          qOr ← query
          q ← qOr \/> "Query shouldn't be empty"
          db ← dbName \/> "DB name shouldn't be empty"
          c ← collectionName \/> "Collection name shouldn't be empty"
          s ← sortQuery
        } yield {
          QuerySetting(q, db, c, s, limit, skip, maxTimeMS)
        }
    }
    f(builder)
    q toProcess builder.build
  }

  //default
  implicit object default extends MongoQuery[MongoClient] {
    override def toProcess(arg: String \/ QuerySetting)(implicit pool: ExecutorService): MongoProcess[MongoClient, DBObject] = {
      arg match {
        case \/-(set) ⇒
          val channel: MongoChannel[MongoClient] = {
            eval(Task now { client: MongoClient ⇒
              Task {
                val logger = Logger.getLogger(Query.getClass)
                scalaz.stream.io.resource(
                  Task delay {
                    val collection = client.getDB(set.db).getCollection(set.collName)
                    val c = collection.find(set.q)
                    set.sortQuery.foreach(c.sort(_))
                    set.skip.foreach(c.skip(_))
                    set.limit.foreach(c.limit(_))
                    set.maxTimeMS.foreach(c.maxTime(_, TimeUnit.MILLISECONDS))
                    logger.debug(s"Cursor: ${c.##} Query: ${set.q} Sort: ${set.sortQuery}")
                    c
                  })(c ⇒ Task.delay(c.close)) { c ⇒
                    Task.delay {
                      if (c.hasNext) {
                        c.next
                      } else {
                        logger.debug(s"Cursor: ${c.##} is exhausted")
                        throw Cause.Terminated(Cause.End)
                      }
                    }
                  }
              }
            })
          }
          MongoProcess(channel)

        case -\/(error) ⇒ MongoProcess(eval(Task.fail(new MongoException(error))))
      }
    }
  }
}