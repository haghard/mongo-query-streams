package mongo.stream

import java.util.concurrent.atomic.AtomicBoolean

import mongo.query.Query._
import org.apache.log4j.Logger
import org.specs2.mutable.Specification

import scalaz.concurrent.Task
import scalaz.stream.Process._

class FailureSpec extends Specification {
  import MongoIntegrationEnv._

  private val logger = Logger.getLogger(classOf[FailureSpec])

  "MongoServer querying" should {
    "hit server with invalid query" in {
      val (sink, _) = sinkWithBuffer[Int]
      val (client, server) = mock()
      val P = eval(Task.delay(client))

      val isFailureInvoked = new AtomicBoolean()
      val isFailureComplete = new AtomicBoolean()

      val products = query { b ⇒
        b.q(""" { "num :  } """)
        b.collection(PRODUCT)
      }.toProcess(DB_NAME)

      val p = for {
        dbObject ← P through (products |> articleIds).channel
        _ ← dbObject to sink
      } yield ()

      //It will be run even it prev process get hail
      p.onFailure { th ⇒ isFailureInvoked.set(true); logger.debug(s"Failure: ${th.getMessage}"); halt }
        .onComplete { eval(Task.delay { isFailureComplete.set(true); logger.info(s"Interaction has been completed") }) }
        .runLog.run

      client.close()
      server.shutdown()

      isFailureInvoked.get must be equalTo true
      isFailureComplete.get must be equalTo true
    }
  }
}