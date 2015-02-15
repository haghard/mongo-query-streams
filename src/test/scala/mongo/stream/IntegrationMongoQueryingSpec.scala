package mongo.stream

import java.util.Date
import mongo.dsl.QueryDsl._
import mongo.dsl.CombinatorDsl._
import mongo.query.Query.query
import org.apache.log4j.Logger
import org.specs2.mutable._
import com.mongodb._
import scalaz.concurrent.Task
import scalaz.stream.Process._
import scala.collection.mutable._
import scalaz.stream.process1

class IntegrationMongoQueryingSpec extends Specification {
  import MongoIntegrationEnv._

  private val logger = Logger.getLogger(classOf[IntegrationMongoQueryingSpec])

  "MongoServer querying" should {
    "hit server with several query by date" in {
      val (sink, buffer) = sinkWithBuffer[Int]
      val (client, server) = mock()
      val P = eval(Task.delay(client))

      val products = query { b ⇒
        b.q("dt" $lt new Date())
        b.collection(PRODUCT)
      }.toProcess(DB_NAME)

      for (i ← 1 to 3) yield {
        val p = for {
          dbObject ← P through (products |> articleIds).channel
          _ ← dbObject to sink
        } yield ()

        //It will be run even it prev process get hail
        p.onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); halt }
          .onComplete { eval(Task.delay(logger.info(s"Interaction $i has been completed"))) }
          .runLog.run
      }

      client.close()
      server.shutdown()
      buffer must be equalTo (ids ++ ids ++ ids)
    }
  }

  "MongoServer querying" should {
    "hit server with multi conditional query" in {
      val Resource = mockDB()
      val (sink, buffer) = sinkWithBuffer[Int]

      val products = query { b ⇒
        b.q("article" $gt 2 $lt 40)
        b.collection(PRODUCT)
      }.toProcess

      val p = for {
        dbObject ← Resource through (products |> articleIds).channel
        _ ← dbObject to sink
      } yield ()

      p.run.run
      buffer must be equalTo ArrayBuffer(3, 35)
    }
  }

  "MongoServer querying" should {
    "hit server with sorted query with folding results in single value" in {
      val Resource = mockDB()
      val (sink, buffer) = sinkWithBuffer[String]

      val products = query { b ⇒
        b.q("article" $gt 2 $lt 40)
        b.sort("article" $eq -1)
        b.collection(PRODUCT)
      }.toProcess

      val p = for {
        dbObject ← Resource through (products |> articleIds0).channel
        _ ← dbObject.fold1 { _ + "-" + _ } to sink
      } yield ()

      p.run.run
      buffer(0) must be equalTo "35-3"
    }
  }

  "MongoServer querying" should {
    "hit server with native json query" in {
      val Resource = mockDB()
      val (sink, buffer) = sinkWithBuffer[String]

      val products = query { b ⇒
        import b._
        q("""{ "article" : 1 } """)
        collection(PRODUCT)
      }.toProcess

      val p = for {
        dbObject ← Resource through (products |> nameTransducer).channel
        _ ← dbObject to sink
      } yield ()

      p.run.run
      buffer(0) must be equalTo "Extra Large Wheel Barrow"
    }
  }

  def producers(id: Int) =
    query { b ⇒
      b.q("prod_num" $eq id)
      b.collection(PRODUCER)
    }.toProcess |> nameTransducer

  def categories(e: (String, Buffer[Int])) = {
    query { b ⇒
      b.q("category" $in e._2)
      b.sort("name" $eq -1)
      b.collection(CATEGORY)
    }.toProcess map { obj ⇒ s"${e._1} - ${obj.get("name").asInstanceOf[String]}" }
  }

  "MongoServer querying" should {
    "hit server with join ONE-TO-ONE" in {
      val Resource = mockDB()
      val (sink, buffer) = sinkWithBuffer[String]
      val products = query { b ⇒
        b.q(""" { "article": 1 } """)
        b.collection(PRODUCT)
      }.toProcess |> numTransducer

      val p = for {
        dbObject ← Resource through (for {
          n ← products
          prod ← producers(n)
        } yield (prod)).channel
        _ ← dbObject to sink
      } yield ()

      p.run.run
      buffer(0) must be equalTo "Reebok"
    }
  }

  "MongoServer querying" should {
    "hit server join ONE-TO-MANY" in {
      val Resource = mockDB()
      val (sink, buffer) = sinkWithBuffer[String]

      val prodsWithCategoryIds = query { b ⇒
        b.q(Obj("article" -> 1).toString)
        b.collection(PRODUCT)
      }.toProcess |> categoryIds

      val p = for {
        dbObject ← Resource through (
          for {
            n ← prodsWithCategoryIds
            prod ← categories(n)
          } yield (prod)).channel
        _ ← dbObject.fold1 { _ + "/" + _ } to sink
      } yield ()

      p.run.run
      buffer(0) must be equalTo "Extra Large Wheel Barrow - Rubberized Work Glove/Extra Large Wheel Barrow - Gardening Tools"
    }
  }

  "MongoServer querying" should {
    "use pipe and separate transducer for parse result " in {
      val Resource = mockDB()
      val (sink, buffer) = sinkWithBuffer[Int]

      val products = query { b ⇒
        b.q(""" { "article": { "$gt" : 0, "$lt" : 4 } }""")
        b.collection(PRODUCT)
        b.limit(2)
        b.skip(1)
      }.toProcess

      val p = for {
        dbObject ← Resource through (products |> articleIds).channel
        _ ← dbObject to sink
      } yield ()

      p.runLog.run
      buffer must be equalTo ArrayBuffer(2, 3)
    }
  }

  "MongoServer querying" should {
    "hit server with zip. Run 2 independent query sequentially and merge results in single value" in {
      //if one process halts, so second process will halt too

      val Resource = mockDB()
      val (sink, buffer) = sinkWithBuffer[String]

      val resultTransducer = process1.lift({ pair: (DBObject, DBObject) ⇒
        pair._1.get("article").asInstanceOf[Int].toString + "-" +
          pair._2.get("category").asInstanceOf[Int].toString
      })

      val products = query { b ⇒
        b.q("article" $eq 1)
        b.collection(PRODUCT)
      }.toProcess

      val categories = query { b ⇒
        b.q("category" $eq 12)
        b.collection(CATEGORY)
      }.toProcess

      val p = for {
        dbObject ← Resource through ((products zip categories) |> resultTransducer).channel
        _ ← dbObject to sink
      } yield ()

      p.runLog.run
      buffer(0) must be equalTo "1-12"
    }
  }

  "MongoServer querying" should {
    "hit server with zipWith" in {
      //if one process halts, so second process will halt too

      val Resource = mockDB()
      val (sink, buffer) = sinkWithBuffer[String]

      val combiner = { (a: DBObject, b: DBObject) ⇒
        a.get("article").asInstanceOf[Int].toString + "-" +
          b.get("category").asInstanceOf[Int].toString
      }

      val products = query { b ⇒
        b.q("article" $in Seq(1, 2, 3))
        b.collection(PRODUCT)
      }.toProcess

      val categories = query { b ⇒
        b.q("category" $in Seq(12, 13))
        b.collection(CATEGORY)
      }.toProcess

      val tee = products.zipWith(categories)(combiner)

      val p = for {
        dbObject ← Resource through tee.channel
        _ ← dbObject to sink
      } yield ()

      p.runLog.run
      buffer must be equalTo ArrayBuffer("1-12", "2-13")
    }
  }

  /*
  "MongoServer querying" should {
    "hit server with query date" in {
      val client = startMockDB()._1

      for (i ← 1 to 5) yield {
        logger.info("Iteration:" + i)

        val db = client.getDB(DB_NAME)
        Thread.sleep(100)
        val q = new BasicDBObject("dt", new BasicDBObject("$lt", new Date()))
        val cursor = db.getCollection(PRODUCT).find(q)
        loop(cursor)

        logger.info("Query: " + q.toString)

        @tailrec
        def loop(c: DBCursor): Unit = c.hasNext match {
          case true ⇒
            logger.info(s"Found: ${c.next()}"); loop(c)
          case false ⇒
        }
      }

      "1" must be equalTo "1"
    }
  }*/

  //TODO - Fold with monoid
}