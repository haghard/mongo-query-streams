/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mongo.query.test

import com.mongodb._
import java.util.Date
import java.util.Arrays._
import java.util.concurrent.{ ThreadLocalRandom, TimeUnit, ExecutorService, Executors }
import de.bwaldvogel.mongo.MongoServer
import de.bwaldvogel.mongo.backend.memory.MemoryBackend
import mongo.{ query, NamedThreadFactory }
import mongo.query.{ MongoStream, MongoStreamFactory, QuerySetting }
import org.apache.log4j.Logger

import scala.collection.JavaConversions._
import scala.collection.mutable.{ Buffer, ArrayBuffer }
import scalaz.stream.Process._
import scalaz.{ -\/, \/-, \/ }
import scalaz.concurrent.Task
import scalaz.stream._

object MongoIntegrationEnv {
  import process1._

  private val logger = org.apache.log4j.Logger.getLogger("mongo-streams")

  implicit val executor = Executors.newFixedThreadPool(5, new NamedThreadFactory("mongo-test-worker"))

  //val asArticleId = lift { obj: DBObject ⇒ obj.get("article").asInstanceOf[Int] }
  //val asArticleIdsStr = lift { obj: DBObject ⇒ obj.get("article").asInstanceOf[Int].toString }
  //val asNameStr = lift { obj: DBObject ⇒ obj.get("name").toString }
  //val asNum = lift { obj: DBObject ⇒ obj.get("producer_num").asInstanceOf[Int] }

  val categoryIds = lift { obj: DBObject ⇒
    (obj.get("name").asInstanceOf[String],
      asScalaBuffer(obj.get("categories").asInstanceOf[java.util.List[Int]]))
  }

  val langs = IndexedSeq("Java", "C++", "ObjectiveC", "Scala", "Groovy")
  def letter = ThreadLocalRandom.current().nextInt('a', 'z').toChar

  val ids = ArrayBuffer(1, 2, 3, 35)

  val TEST_DB = "temp"
  val PRODUCT = "product"
  val CATEGORY = "category"
  val PRODUCER = "producer"
  val PROGRAMMERS = "programmers"
  val LANGS = "langs"

  def sinkWithBuffer[T] = {
    val buffer: Buffer[T] = Buffer.empty
    (scalaz.stream.io.fillBuffer(buffer), buffer)
  }

  def LoggerSink(logger: Logger): Sink[Task, String] =
    scalaz.stream.sink.lift[Task, String](o ⇒ Task.delay(logger.debug(s"Result:  $o")))

  private[test] def prepareMockMongo(): (MongoClient, MongoServer) = {
    val server = new MongoServer(new MemoryBackend())
    val serverAddress = server.bind()
    val client = new MongoClient(new ServerAddress(serverAddress))
    val products = client.getDB(TEST_DB).getCollection(PRODUCT)

    val langsC = client.getDB(TEST_DB).getCollection(LANGS)
    val programmers = client.getDB(TEST_DB).getCollection(PROGRAMMERS)

    for ((v, i) ← langs.zipWithIndex) {
      langsC.insert(new BasicDBObject("index", i).append("name", v))
    }

    for (i ← 1 to 10) {
      programmers.insert(new BasicDBObject("name", s"$letter-$letter-$letter")
        .append("lang", ThreadLocalRandom.current().nextInt(langs.size)))
    }

    products.insert(new BasicDBObject("article", ids(0)).append("name", "Extra Large Wheel Barrow")
      .append("producer_num", 1)
      .append("categories", asList(12, 13, 14))
      .append("dt", new Date()))

    products.insert(new BasicDBObject("article", ids(1)).append("name", "Large Wheel Barrow")
      .append("producer_num", 2)
      .append("dt", new Date()).append("category", asList(13))
      .append("f", true).append("categories", asList(12, 15)))

    products.insert(new BasicDBObject("article", ids(2)).append("name", "Medium Wheel Barrow").append("dt", new Date())
      .append("category", asList(13)).append("f", true))
    products.insert(new BasicDBObject("article", ids(3)).append("name", "Small Wheel Barrow").append("dt", new Date())
      .append("category", asList(13)).append("f", true))

    val producer = client.getDB(TEST_DB).getCollection(PRODUCER)
    producer.insert(new BasicDBObject().append("producer_num", 1).append("name", "Puma"))
    producer.insert(new BasicDBObject().append("producer_num", 1).append("name", "Reebok"))
    producer.insert(new BasicDBObject().append("producer_num", 2).append("name", "Adidas"))

    val category = client.getDB(TEST_DB).getCollection(CATEGORY)
    category.insert(new BasicDBObject().append("category", 12).append("name", "Gardening Tools"))
    category.insert(new BasicDBObject().append("category", 13).append("name", "Rubberized Work Glove"))
    category.insert(new BasicDBObject().append("category", 14).append("name", "Car Tools"))
    category.insert(new BasicDBObject().append("category", 15).append("name", "Car1 Tools"))
    (client, server)
  }

  def mock(): (MongoClient, MongoServer) = prepareMockMongo()

  def mockDB()(implicit executor: java.util.concurrent.ExecutorService): scalaz.stream.Process[Task, DB] = {
    scalaz.stream.io.resource(Task.delay(prepareMockMongo()))(rs ⇒ Task.delay {
      rs._1.close
      rs._2.shutdownNow
      logger.debug(s"mongo-client ${rs._1.##} has been closed")
    }) { rs ⇒
      var obtained = false
      Task.fork(
        Task.delay {
          if (!obtained) {
            val db = rs._1.getDB(TEST_DB)
            logger.debug(s"Access mongo-client ${rs._1.##}")
            obtained = true
            db
          } else throw Cause.Terminated(Cause.End)
        }
      )(executor)
    }
  }

  /**
   * used in test cases
   */
  implicit object TestCaseFactory extends MongoStreamFactory[DB] {
    override def createMStream(arg: String \/ QuerySetting)(implicit pool: ExecutorService): MongoStream[DB, DBObject] = {
      arg match {
        case \/-(set) ⇒
          query.MongoStream {
            eval(Task.now { db: DB ⇒
              Task {
                scalaz.stream.io.resource(
                  Task delay {
                    val collection = db.getCollection(set.collName)
                    val cursor = collection.find(set.q)
                    set.sortQuery.foreach(cursor.sort(_))
                    set.skip.foreach(cursor.skip(_))
                    set.limit.foreach(cursor.limit(_))
                    set.maxTimeMS.foreach(cursor.maxTime(_, TimeUnit.MILLISECONDS))
                    logger.debug(s"Cursor: ${cursor.##} Query: ${set.q} Sort: ${set.sortQuery}")
                    cursor
                  })(cursor ⇒ Task.delay(cursor.close)) { c ⇒
                    Task.delay {
                      if (c.hasNext) {
                        Thread.sleep(200) //for test
                        val r = c.next
                        logger.debug(r)
                        r
                      } else {
                        logger.debug(s"Cursor: ${c.##} is exhausted")
                        throw Cause.Terminated(Cause.End)
                      }
                    }
                  }
              }(pool)
            })
          }
        case -\/(error) ⇒ query.MongoStream(eval(Task.fail(new MongoException(error))))
      }
    }
  }
}
