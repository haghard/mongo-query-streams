package mongo.streams

import java.util.concurrent.ExecutorService

import com.mongodb.{ DB, DBObject, MongoClient }

private[mongo] trait MongoQuery {

  def toProcess(implicit pool: ExecutorService): MongoProcess[DB, DBObject]

  def toProcess(dbName: String)(implicit pool: ExecutorService): MongoProcess[MongoClient, DBObject]
}
