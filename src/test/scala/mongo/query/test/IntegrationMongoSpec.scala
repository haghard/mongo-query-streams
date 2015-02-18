package mongo.query.test

import mongo.dsl._
import java.util.Date
import mongo.query.query
import scalaz.concurrent.Task
import org.apache.log4j.Logger
import scalaz.stream.Process._
import java.util.concurrent.atomic.AtomicBoolean
import MongoIntegrationEnv.{ executor, ids, sinkWithBuffer, mock, articleIds, articleIds0, DB_NAME, PRODUCT }

import org.specs2.mutable._
import org.specs2.specification.Snippets

trait TestEnviroment[T] extends org.specs2.mutable.After {
  protected val logger = Logger.getLogger(classOf[IntegrationMongoSpec])

  val (sink, buffer) = sinkWithBuffer[T]
  val isFailureInvoked = new AtomicBoolean()
  val isFailureComplete = new AtomicBoolean()

  lazy val server = mock()

  def EnvLogger(): scalaz.stream.Sink[Task, String] = MongoIntegrationEnv.LoggerSink(logger)

  /**
   * Start mock mongo and return Process
   * @return
   */
  def Resource = {
    server
    eval(Task.delay(server._1))
  }

  override def after = {
    server._1.close
    server._2.shutdown
  }
}

class IntegrationMongoSpec extends Specification with Snippets {

  "Hit server with invalid query" in new TestEnviroment[Int] {
    val q = query { b ⇒
      b.q(""" { "num :  } """)
      b.db(DB_NAME)
      b.collection(PRODUCT)
    }

    val p = for {
      dbObject ← Resource through (q |> articleIds).channel
      _ ← dbObject to sink
    } yield ()

    //It will be run even it prev process get hail
    p.onFailure { th ⇒ isFailureInvoked.set(true); halt }
      .onComplete { eval(Task.delay(isFailureComplete.set(true))) }
      .runLog.run

    isFailureInvoked.get && isFailureComplete.get must be equalTo true
  }

  "Hit server with invalid query - missing collection" in new TestEnviroment[Int] {
    val q = query { b ⇒
      b.q(""" { "num" : 1 } """)
      b.db(DB_NAME)
    }

    val p = for {
      dbObject ← Resource through (q |> articleIds).channel
      _ ← dbObject to sink
    } yield ()

    p.onFailure { th ⇒ isFailureInvoked.set(true); logger.debug(th.getMessage); halt }
      .onComplete(eval(Task.delay(isFailureComplete.set(true))))
      .runLog.run

    isFailureInvoked.get && isFailureComplete.get must be equalTo true
  }

  "Hit server with invalid query - invalid sorting" in new TestEnviroment[Int] {
    val q = query { b ⇒
      b.q(""" { "num" : 1 } """)
      b.sort(""" { "num } """) //invalid
      b.collection(PRODUCT)
      b.db(DB_NAME)
    }

    val p = for {
      dbObject ← Resource through (q |> articleIds).channel
      _ ← dbObject to sink
    } yield ()

    p.onFailure { th ⇒ isFailureInvoked.set(true); logger.debug(th.getMessage); halt }
      .onComplete(eval(Task.delay(isFailureComplete.set(true))))
      .runLog.run

    isFailureInvoked.get && isFailureComplete.get must be equalTo true
  }

  "Hit server with invalid query - missing db" in new TestEnviroment[Int] {
    val q = query { b ⇒
      b.q(""" { "num" : 1 } """)
      b.collection(PRODUCT)
    }

    val p = for {
      dbObject ← Resource through (q |> articleIds).channel
      _ ← dbObject to sink
    } yield ()

    p.onFailure { th ⇒ isFailureInvoked.set(true); logger.debug(th.getMessage); halt }
      .onComplete(eval(Task.delay(isFailureComplete.set(true))))
      .runLog.run

    isFailureInvoked.get && isFailureComplete.get must be equalTo true
  }

  "Hit server several times with the same query by date" in new TestEnviroment[Int] {
    val products = query { b ⇒
      b.q("dt" $gt new Date())
      b.collection(PRODUCT)
      b.db(DB_NAME)
    }

    for (i ← 1 to 3) yield {
      val p = for {
        dbObject ← Resource through (products |> articleIds).channel
        _ ← dbObject to sink
      } yield ()

      //It will be run even it prev process get hail
      p.onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); halt }
        .onComplete { eval(Task.delay(logger.debug(s"Interaction $i has been completed"))) }
        .runLog.run
    }

    buffer must be equalTo (ids ++ ids ++ ids)
  }

  "Hit server with monadic query" in new TestEnviroment[String] {
    import mongo.dsl._
    import free._

    val program = for {
      _ ← "article" $gt 0 $lt 4
      x ← "producer_num" $gt 0
    } yield x

    val products = query { b ⇒
      b.q(instructions(program))
      b.collection(PRODUCT)
      b.db(DB_NAME)
    }

    val p = for {
      dbObject ← Resource through (products |> articleIds0).channel
      _ ← dbObject observe EnvLogger to sink
    } yield ()

    //It will be run even it prev process get hail
    p.onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); halt }
      .onComplete { eval(Task.delay(logger.debug(s"Interaction has been completed"))) }
      .runLog.run

    buffer(0) must be equalTo "1"
    buffer(1) must be equalTo "2"
  }
}