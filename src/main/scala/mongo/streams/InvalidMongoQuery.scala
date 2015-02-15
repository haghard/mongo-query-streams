package mongo.streams

import java.util.concurrent.ExecutorService
import com.mongodb.{ MongoException, DB, DBObject, MongoClient }

import scalaz.concurrent.Task
import scalaz.stream.Process._

private[mongo] class InvalidMongoQuery(error: String) extends MongoQuery {

  override def toProcess(implicit pool: ExecutorService): MongoProcess[DB, DBObject] =
    MongoProcess {
      eval(Task.now { db: DB ⇒
        Task(eval(Task.delay[DBObject](throw new MongoException(error))))
      })
    }
  
  override def toProcess(dbName: String)(implicit pool: ExecutorService): MongoProcess[MongoClient, DBObject] = 
    MongoProcess {
      eval(Task.now { client: MongoClient ⇒
        Task(eval(Task.delay[DBObject](throw new MongoException(error))))
      })
    }
}