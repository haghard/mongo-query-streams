package mongo.streams

import java.util.concurrent.{ ExecutorService, TimeUnit }

import com.mongodb.{ MongoClient, DB, DBObject }
import org.apache.log4j.Logger

import scalaz.concurrent.Task
import scalaz.stream.Process.eval
import scalaz.stream.{ Cause, Channel, Process, io }

/**
 *
 * @param queryDBObject
 * @param collectionName
 * @param limit
 * @param skip
 * @param maxTime
 */
private[mongo] class DefaultMongoQuery(queryDBObject: DBObject, collectionName: String,
                                       sortQuery: Option[DBObject] = None, limit: Option[Int] = None,
                                       skip: Option[Int] = None, maxTime: Option[Long] = None) extends MongoQuery {

  private val logger = Logger.getLogger(classOf[DefaultMongoQuery])

  override def toProcess(dbName: String)(implicit pool: ExecutorService): MongoProcess[MongoClient, DBObject] = {
    val channel: Channel[Task, MongoClient, Process[Task, DBObject]] = {
      eval(Task now { client: MongoClient ⇒
        Task {
          io.resource(
            Task delay {
              val collection = client.getDB(dbName) getCollection collectionName
              val cursor = collection find queryDBObject
              logger debug s"Cursor: ${cursor.##} Query: $queryDBObject"
              sortQuery foreach (cursor.sort(_))
              skip foreach (cursor.skip(_))
              limit foreach (cursor.limit(_))
              maxTime foreach (cursor.maxTime(_, TimeUnit.MILLISECONDS))
              cursor
            })(cursor ⇒ Task.delay(cursor.close)) { c ⇒
              Task.delay {
                if (c.hasNext) {
                  c.next
                } else {
                  logger debug s"Cursor: ${c.##} is exhausted"
                  throw Cause.Terminated(Cause.End)
                }
              }
            }
        }
      })
    }
    MongoProcess(channel)
  }

  override def toProcess(implicit pool: ExecutorService): MongoProcess[DB, DBObject] = {
    val channel: Channel[Task, DB, Process[Task, DBObject]] =
      eval(Task now { db: DB ⇒
        Task {
          io.resource(
            Task delay {
              val collection = db getCollection collectionName
              val cursor = collection find queryDBObject
              logger debug s"Cursor: ${cursor.##} Query: $queryDBObject"
              sortQuery foreach (cursor.sort(_))
              skip foreach (cursor.skip(_))
              limit foreach (cursor.limit(_))
              maxTime foreach (cursor.maxTime(_, TimeUnit.MILLISECONDS))
              cursor
            })(cursor ⇒ Task.delay(cursor.close)) { c ⇒
              Task.delay {
                if (c.hasNext) {
                  c.next
                } else {
                  logger debug s"Cursor: ${c.##} is exhausted"
                  throw Cause.Terminated(Cause.End)
                }
              }
            }
        }
      })
    MongoProcess(channel)
  }
}