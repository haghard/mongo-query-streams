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
import org.specs2.mutable.Specification

import scala.collection.mutable.Buffer
import scala.concurrent.ExecutionContext
import scalaz.concurrent.Task
import scalaz.stream.{ io, Process }

import mongo.join.process.MongoProcess
import mongo.join.observable.MongoObservable
import rx.lang.scala.Subscriber
import rx.lang.scala.schedulers.ExecutionContextScheduler

class JoinerSpec extends Specification {
  import mongo._
  import join._
  import dsl._
  import qb._
  import MongoIntegrationEnv._

  "Build joinByPk with Process" in new MongoStreamsEnviroment {
    initMongo

    val buffer = Buffer.empty[String]
    val Sink = io.fillBuffer(buffer)

    val qLang = for {
      _ ← "index" $gte 0 $lte 5
      q ← limit(5)
    } yield q

    def qProg(id: Int) = for { q ← "lang" $eq id } yield q

    implicit val c = client
    val joiner = Join[MongoProcess]

    val query = joiner.joinByPk(qLang, LANGS, "index", qProg(_: Int), PROGRAMMERS, TEST_DB) { (l, r: DBObject) ⇒
      s"Primary-key:$l - val:[Foreign-key:${r.get("lang")} - ${r.get("name")}]"
    }

    val p = for {
      e ← Process.eval(Task.delay(client)) through query.out
      _ ← e to Sink
    } yield ()

    p.runLog.run
    buffer.size === MongoIntegrationEnv.progSize
  }

  "Build join with Process" in new MongoStreamsEnviroment {
    initMongo

    val buffer = Buffer.empty[String]
    val Sink = io.fillBuffer(buffer)

    val qLang = for { q ← "index" $gte 0 $lte 5 } yield q
    def qProg(left: DBObject) = for { q ← "lang" $eq left.get("index").asInstanceOf[Int] } yield q

    implicit val c = client
    val joiner = Join[MongoProcess]

    val query = joiner.join(qLang, LANGS, qProg(_: DBObject), PROGRAMMERS, TEST_DB) { (l, r) ⇒
      s"Primary-key:${l.get("index")} - val:[Foreign-key:${r.get("lang")} - ${r.get("name")}]"
    }

    val p = for {
      e ← Process.eval(Task.delay(client)) through query.out
      _ ← e to Sink
    } yield ()

    p.runLog.run
    buffer.size === MongoIntegrationEnv.progSize
  }

  "Build joinByPk with Observable" in new MongoStreamsEnviroment {
    initMongo

    val count = new CountDownLatch(1)
    val responses = new AtomicLong(0)

    val qLang = for {
      _ ← "index" $gte 0 $lte 5
      q ← limit(5)
    } yield q

    def qProg(id: Int) = for { q ← "lang" $eq id } yield q

    implicit val c = client
    val joiner = Join[MongoObservable]

    val query = joiner.joinByPk(qLang, LANGS, "index", qProg(_: Int), PROGRAMMERS, TEST_DB) { (l, r: DBObject) ⇒
      s"Primary-key:$l - val:[Foreign-key:${r.get("lang")} - ${r.get("name")}]"
    }

    val S = new Subscriber[String] {
      override def onStart(): Unit = request(1)
      override def onNext(n: String): Unit = {
        logger.info(s"onNext $n")
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
      .subscribe(S)

    count.await()
    responses.get === MongoIntegrationEnv.progSize
  }

  "Build join with Observable" in new MongoStreamsEnviroment {
    initMongo

    val count = new CountDownLatch(1)
    val responses = new AtomicLong(0)

    val qLang = for { q ← "index" $gte 0 $lte 5 } yield q
    def qProg(left: DBObject) = for { q ← "lang" $eq left.get("index").asInstanceOf[Int] } yield q

    implicit val c = client
    val joiner = Join[MongoObservable]

    val query = joiner.join(qLang, LANGS, qProg(_: DBObject), PROGRAMMERS, TEST_DB) { (l, r) ⇒
      s"Primary-key:${l.get("index")} - val:[Foreign-key:${r.get("lang")} - ${r.get("name")}]"
    }

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
    responses.get === MongoIntegrationEnv.progSize
  }
}