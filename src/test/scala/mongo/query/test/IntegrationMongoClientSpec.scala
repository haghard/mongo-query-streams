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

import mongo._
import dsl._
import qb._
import java.util.Date
import mongo.query.create
import scala.collection.mutable.ArrayBuffer
import scalaz.\/
import scalaz.concurrent.Task
import org.apache.log4j.Logger
import scalaz.stream.Process._
import java.util.concurrent.atomic.AtomicBoolean
import MongoIntegrationEnv.{ executor, ids, sinkWithBuffer, mock, TEST_DB, PRODUCT, CATEGORY }
import org.specs2.mutable._

trait TestEnviroment[T] extends org.specs2.mutable.After {
  protected val logger = Logger.getLogger(classOf[IntegrationMongoClientSpec])

  val (sink, buffer) = sinkWithBuffer[T]
  val isFailureInvoked = new AtomicBoolean()
  val isFailureComplete = new AtomicBoolean()

  lazy val server = mock()

  def EnvLogger() = MongoIntegrationEnv.LoggerSink(logger)

  def EnvLoggerEither() = MongoIntegrationEnv.LoggerSinkEither(logger)

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

class IntegrationMongoClientSpec extends Specification {

  "Hit server with invalid query" in new TestEnviroment[Int] {
    val q = create { b ⇒
      b.q(""" { "num :  } """)
      b.db(TEST_DB)
      b.collection(PRODUCT)
    }.column[Int]("article")

    (for {
      dbObject ← Resource through q.out
      _ ← dbObject to sink
    } yield ())
      .onFailure { th ⇒ isFailureInvoked.set(true); halt }
      .onComplete { eval(Task.delay(isFailureComplete.set(true))) }
      .runLog.run

    isFailureInvoked.get && isFailureComplete.get must be equalTo true
  }

  "Hit server with invalid query - missing collection" in new TestEnviroment[Int] {
    val q = create { b ⇒
      b.q(""" { "num" : 1 } """)
      b.db(TEST_DB)
    }.column[Int]("article")

    (for {
      dbObject ← Resource through q.out
      _ ← dbObject to sink
    } yield ())
      .onFailure { th ⇒ isFailureInvoked.set(true); logger.debug(th.getMessage); halt }
      .onComplete(eval(Task.delay(isFailureComplete.set(true))))
      .runLog.run

    isFailureInvoked.get && isFailureComplete.get must be equalTo true
  }

  "Hit server with invalid query - invalid sorting" in new TestEnviroment[Int] {
    val q = create { b ⇒
      b.q(""" { "num" : 1 } """)
      b.sort(""" { "num } """) //invalid
      b.collection(PRODUCT)
      b.db(TEST_DB)
    }.column[Int]("article")

    (for {
      dbObject ← Resource through q.out
      _ ← dbObject to sink
    } yield ())
      .onFailure { th ⇒ isFailureInvoked.set(true); logger.debug(th.getMessage); halt }
      .onComplete(eval(Task.delay(isFailureComplete.set(true))))
      .runLog.run

    isFailureInvoked.get && isFailureComplete.get must be equalTo true
  }

  "Hit server with invalid query - missing db" in new TestEnviroment[Int] {
    val q = create { b ⇒
      b.q(""" { "num" : 1 } """)
      b.collection(PRODUCT)
    }.column[Int]("article")

    (for {
      dbObject ← Resource through q.out
      _ ← dbObject to sink
    } yield ())
      .onFailure { th ⇒ isFailureInvoked.set(true); logger.debug(th.getMessage); halt }
      .onComplete(eval(Task.delay(isFailureComplete.set(true))))
      .runLog.run

    isFailureInvoked.get && isFailureComplete.get must be equalTo true
  }

  "Hit server several times with the same query by date" in new TestEnviroment[Int] {
    val products = create { b ⇒
      b.q("dt" $gt new Date())
      b.collection(PRODUCT)
      b.db(TEST_DB)
    }.column[Int]("article")

    for (i ← 1 to 3) yield {
      (for {
        dbObject ← Resource through products.out
        _ ← dbObject to sink
      } yield ())
        .onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); halt }
        .onComplete { eval(Task.delay(logger.debug(s"Interaction $i has been completed"))) }
        .runLog.run
    }

    buffer must be equalTo (ids ++ ids ++ ids)
  }

  "Hit server with monadic query to instructions" in new TestEnviroment[String] {
    val program = for {
      _ ← "article" $gt 0 $lt 4
      x ← "producer_num" $gt 0
    } yield x

    val products = create { b ⇒
      b.q(program.toQuery)
      b.collection(PRODUCT)
      b.db(TEST_DB)
    }.column[Int]("article").map(_.toString)

    (for {
      dbObject ← Resource through products.out
      _ ← dbObject observe EnvLogger to sink
    } yield ())
      .onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); halt }
      .onComplete { eval(Task.delay(logger.debug(s"Interaction has been completed"))) }
      .runLog.run

    buffer must be equalTo ArrayBuffer("1", "2")
  }

  "Hit server with monadic query2" in new TestEnviroment[String] {
    val producers = for {
      _ ← "article" $gt 0 $lt 4
      x ← "producer_num" $gt 0
    } yield x

    val products = create { b ⇒
      b.q(producers.toDBObject)
      b.collection(PRODUCT)
      b.db(TEST_DB)
    }.column[Int]("article").map(_.toString)

    (for {
      dbObject ← Resource through products.out
      _ ← dbObject observe EnvLogger to sink
    } yield ())
      .onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); halt }
      .onComplete { eval(Task.delay(logger.debug(s"Interaction has been completed"))) }
      .runLog.run

    buffer must be equalTo ArrayBuffer("1", "2")
  }

  "Interleave query streams nondeterminstically" in new TestEnviroment[String \/ Int] {

    val products = create { b ⇒
      b.q("article" $in Seq(1, 2, 3))
      b.collection(PRODUCT)
      b.db(TEST_DB)
    }.column[Int]("article").map(_.toString)

    val categories = create { b ⇒
      b.q("category" $in Seq(12, 13))
      b.collection(CATEGORY)
      b.db(TEST_DB)
    }.column[Int]("category")

    (for {
      cats ← Resource through categories.out
      prodOrCat ← Resource through ((products either cats).out)
      _ ← prodOrCat observe EnvLoggerEither to sink
    } yield ())
      .onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); halt }
      .onComplete { eval(Task.delay(logger.debug(s"Interaction has been completed"))) }
      .runLog.run

    buffer.size === 5
  }
}