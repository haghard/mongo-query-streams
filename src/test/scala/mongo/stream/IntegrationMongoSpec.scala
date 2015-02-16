package mongo.stream

import java.util.Date
import java.util.concurrent.atomic.AtomicBoolean
import mongo.query.Query._
import org.apache.log4j.Logger
import org.specs2.mutable.Specification
import scalaz.concurrent.Task
import scalaz.stream.Process._
import mongo.dsl._

class IntegrationMongoSpec extends Specification {
  import MongoIntegrationEnv.{ executor, ids, sinkWithBuffer, mock, articleIds, DB_NAME, PRODUCT }
  import mongo.query.Query.default

  private val logger = Logger.getLogger(classOf[IntegrationMongoSpec])

  "MongoServer querying" should {
    "hit server with invalid query" in {
      val (sink, _) = sinkWithBuffer[Int]
      val (client, server) = mock()
      val P = eval(Task.delay(client))

      val isFailureInvoked = new AtomicBoolean()
      val isFailureComplete = new AtomicBoolean()

      val q = query { b ⇒
        b.q(""" { "num :  } """)
        b.db(DB_NAME)
        b.collection(PRODUCT)
      }

      val p = for {
        dbObject ← P through (q |> articleIds).channel
        _ ← dbObject to sink
      } yield ()

      //It will be run even it prev process get hail
      p.onFailure { th ⇒ isFailureInvoked.set(true); halt }
        .onComplete { eval(Task.delay(isFailureComplete.set(true))) }
        .runLog.run

      client.close()
      server.shutdown()

      isFailureInvoked.get && isFailureComplete.get must be equalTo true
    }
  }

  "MongoServer querying" should {
    "hit server with invalid query - missing collection" in {
      val isFailureInvoked = new AtomicBoolean()
      val isFailureComplete = new AtomicBoolean()

      val (sink, _) = sinkWithBuffer[Int]
      val (client, server) = mock()
      val P = eval(Task.delay(client))

      val q = query { b ⇒
        b.q(""" { "num" : 1 } """)
        b.db(DB_NAME)
      }

      val p = for {
        dbObject ← P through (q |> articleIds).channel
        _ ← dbObject to sink
      } yield ()

      p.onFailure { th ⇒ isFailureInvoked.set(true); logger.debug(th.getMessage); halt }
        .onComplete(eval(Task.delay(isFailureComplete.set(true))))
        .runLog.run

      client.close()
      server.shutdown()

      isFailureInvoked.get && isFailureComplete.get must be equalTo true
    }
  }

  "MongoServer querying" should {
    "hit server with invalid query - invalid sorting" in {
      val (client, server) = mock()
      val P = eval(Task.delay(client))
      val (sink, _) = sinkWithBuffer[Int]

      val isFailureInvoked = new AtomicBoolean()
      val isFailureComplete = new AtomicBoolean()

      val q = query { b ⇒
        b.q(""" { "num" : 1 } """)
        b.sort(""" { "num } """) //invalid
        b.collection(PRODUCT)
        b.db(DB_NAME)
      }

      val p = for {
        dbObject ← P through (q |> articleIds).channel
        _ ← dbObject to sink
      } yield ()

      p.onFailure { th ⇒ isFailureInvoked.set(true); logger.debug(th.getMessage); halt }
        .onComplete(eval(Task.delay(isFailureComplete.set(true))))
        .runLog.run

      client.close
      server.shutdown

      isFailureInvoked.get && isFailureComplete.get must be equalTo true
    }
  }

  "MongoServer querying" should {
    "hit server with invalid query - missing db" in {
      val (client, server) = mock()
      val P = eval(Task.delay(client))
      val (sink, _) = sinkWithBuffer[Int]

      val isFailureInvoked = new AtomicBoolean()
      val isFailureComplete = new AtomicBoolean()

      val q = query { b ⇒
        b.q(""" { "num" : 1 } """)
        b.collection(PRODUCT)
      }

      val p = for {
        dbObject ← P through (q |> articleIds).channel
        _ ← dbObject to sink
      } yield ()

      p.onFailure { th ⇒ isFailureInvoked.set(true); logger.debug(th.getMessage); halt }
        .onComplete(eval(Task.delay(isFailureComplete.set(true))))
        .runLog.run

      client.close
      server.shutdown

      isFailureInvoked.get && isFailureComplete.get must be equalTo true
    }
  }

  "MongoServer querying" should {
    "hit server with several query by date" in {
      val (sink, buffer) = sinkWithBuffer[Int]
      val (client, server) = mock()
      val P = eval(Task.delay(client))

      val products = query { b ⇒
        b.q("dt" $lt new Date())
        b.collection(PRODUCT)
        b.db(DB_NAME)
      }

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
}