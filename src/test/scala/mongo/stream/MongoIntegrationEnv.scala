package mongo.stream

import java.util.Arrays._
import java.util.Date
import java.util.concurrent.Executors

import com.mongodb._
import de.bwaldvogel.mongo.MongoServer
import de.bwaldvogel.mongo.backend.memory.MemoryBackend
import mongo.NamedThreadFactory
import org.apache.log4j.Logger

import scala.collection.JavaConversions._
import scala.collection.mutable.{ Buffer, ArrayBuffer }
import scalaz.concurrent.Task
import scalaz.stream.{ process1, Cause, io }

object MongoIntegrationEnv {

  private val logger = Logger.getLogger(this.getClass)

  implicit val mongoExecutor = Executors.newFixedThreadPool(10, new NamedThreadFactory("mongo-worker"))

  val articleIds = process1.lift({ obj: DBObject ⇒ obj.get("article").asInstanceOf[Int] })

  val articleIds0 = process1.lift({ obj: DBObject ⇒ obj.get("article").asInstanceOf[Int].toString })

  val nameTransducer = process1.lift({ obj: DBObject ⇒ obj.get("name").toString })

  val numTransducer = process1.lift({ obj: DBObject ⇒ obj.get("prod_num").asInstanceOf[Int] })

  val categoryIds = process1.lift({ obj: DBObject ⇒
    (obj.get("name").asInstanceOf[String], asScalaBuffer(obj.get("categories").asInstanceOf[java.util.List[Int]]))
  })

  val ids = ArrayBuffer(1, 2, 3, 35)

  val DB_NAME = "temp"
  val PRODUCT = "product"
  val CATEGORY = "category"
  val PRODUCER = "producer"

  def sinkWithBuffer[T] = {
    val buffer: Buffer[T] = Buffer.empty
    (io.fillBuffer(buffer), buffer)
  }

  private def prepareMockMongo(): (MongoClient, MongoServer) = {
    val server = new MongoServer(new MemoryBackend())
    val serverAddress = server.bind()
    val client = new MongoClient(new ServerAddress(serverAddress))
    val products = client.getDB(DB_NAME).getCollection(PRODUCT)

    products.insert(new BasicDBObject("article", ids(0)).append("name", "Extra Large Wheel Barrow")
      .append("prod_num", 1)
      .append("categories", asList(12, 13)).append("dt", new Date()))

    products.insert(new BasicDBObject("article", ids(1)).append("name", "Large Wheel Barrow").append("dt", new Date())
      .append("category", asList(13)).append("f", true))
    products.insert(new BasicDBObject("article", ids(2)).append("name", "Medium Wheel Barrow").append("dt", new Date())
      .append("category", asList(13)).append("f", true))
    products.insert(new BasicDBObject("article", ids(3)).append("name", "Small Wheel Barrow").append("dt", new Date())
      .append("category", asList(13)).append("f", true))

    val producer = client.getDB(DB_NAME).getCollection(PRODUCER)
    producer.insert(new BasicDBObject().append("prod_num", 1).append("name", "Reebok"))
    producer.insert(new BasicDBObject().append("prod_num", 2).append("name", "Adidas"))

    val category = client.getDB(DB_NAME).getCollection(CATEGORY)
    category.insert(new BasicDBObject().append("category", 12).append("name", "Gardening Tools"))
    category.insert(new BasicDBObject().append("category", 13).append("name", "Rubberized Work Glove"))
    (client, server)
  }

  def mock() = prepareMockMongo()

  def mockDB()(implicit executor: java.util.concurrent.ExecutorService): scalaz.stream.Process[Task, DB] = {
    io.resource(Task.delay(prepareMockMongo()))(rs ⇒ Task.delay {
      rs._1.close
      rs._2.shutdownNow
      logger debug s"mongo-client ${rs._1.##} has been closed"
    }) { rs ⇒
      var obtained = false
      Task.fork(
        Task.delay {
          if (!obtained) {
            val db = rs._1.getDB(DB_NAME)
            logger debug s"Access mongo-client ${rs._1.##}"
            obtained = true
            db
          } else throw Cause.Terminated(Cause.End)
        }
      )(executor)
    }
  }
}