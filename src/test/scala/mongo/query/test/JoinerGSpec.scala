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

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong

import com.mongodb.DBObject
import join.observable.MongoObservableStream
import join.process.MongoProcessStream
import org.specs2.mutable.Specification
import rx.lang.scala.Subscriber
import rx.lang.scala.schedulers.ExecutionContextScheduler

import scala.collection.mutable.Buffer
import scala.concurrent.ExecutionContext
import scalaz.concurrent.Task
import scalaz.stream.{ Process, io }

class JoinerGSpec extends Specification {
  import mongo._
  import join._
  import dsl._
  import qb._
  import MongoIntegrationEnv._

  "JoinG with MongoProcessStream" in new MongoEnviromentLifecycle {
    initMongo

    val buffer = Buffer.empty[String]
    val Sink = io.fillBuffer(buffer)

    val qLang = for { q ← "index" $gte 0 $lte 5 } yield q
    def qProg(left: DBObject) = for { q ← "lang" $eq left.get("index").asInstanceOf[Int] } yield q

    implicit val c = client
    val joiner = JoinG[MongoProcessStream]

    val query = joiner.join(qLang, LANGS, qProg(_), PROGRAMMERS, TEST_DB) { (l, r) ⇒
      s"Primary-key:${l.get("index")} - val:[Foreign-key:${r.get("lang")} - ${r.get("name")}]"
    }

    val p = for {
      e ← Process.eval(Task.delay(client)) through query.out
      _ ← e to Sink
    } yield ()

    p.run.run
    buffer.size === 10
  }

  "JoinG with MongoObservableStream" in new MongoEnviromentLifecycle {
    initMongo

    val buffer = Buffer.empty[String]
    val Sink = io.fillBuffer(buffer)

    val qLang = for { q ← "index" $gte 0 $lte 5 } yield q
    def qProg(left: DBObject) = for { q ← "lang" $eq left.get("index").asInstanceOf[Int] } yield q

    implicit val c = client
    val joiner = JoinG[MongoObservableStream]

    val query = joiner.join(qLang, LANGS, qProg(_), PROGRAMMERS, TEST_DB) { (l, r) ⇒
      s"Primary-key:${l.get("index")} - val:[Foreign-key:${r.get("lang")} - ${r.get("name")}]"
    }

    val count = new CountDownLatch(1)
    val responses = new AtomicLong(0)
    val testSubs = new Subscriber[String] {
      override def onStart(): Unit = request(1)
      override def onNext(n: String): Unit = {
        logger.info(s"receive $n")
        responses.incrementAndGet()
        request(1)
      }
      override def onError(e: Throwable): Unit = {
        logger.info(s"OnError: ${e.getMessage}")
        count.countDown()
      }
      override def onCompleted(): Unit = {
        logger.info("Interaction has been completed")
        count.countDown()
      }
    }

    query.observeOn(ExecutionContextScheduler(ExecutionContext.fromExecutor(executor)))
      .subscribe(testSubs)

    count.await()
    responses.get === 10
  }
}