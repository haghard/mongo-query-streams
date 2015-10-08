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

package mongo.query.test.observable

import java.util.concurrent.atomic.{ AtomicLong, AtomicInteger }
import java.util.concurrent.CountDownLatch

import com.mongodb.DBObject
import mongo.query.test.{ MongoStreamsEnviroment, MongoIntegrationEnv }
import org.specs2.mutable.Specification
import rx.lang.scala.schedulers.ExecutionContextScheduler
import rx.lang.scala.{ Observable, Subscriber }

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scalaz.stream.io

class ObservableSpec extends Specification {
  import mongo._
  import mongo.join.observable._
  import dsl._
  import qb._
  import MongoIntegrationEnv._

  val RxExecutor = ExecutionContextScheduler(ExecutionContext.fromExecutor(executor))

  "Build query and perform streaming using Observable" in new MongoStreamsEnviroment {
    initMongo()

    implicit val cl = client
    val latch = new CountDownLatch(1)
    val mongoQuery = for { ex ← "index" $gte 0 } yield ex

    val counter = new AtomicLong(0)
    val s = new rx.lang.scala.Subscriber[DBObject] {
      val pageSize = 8
      override def onStart() = request(pageSize)
      override def onNext(n: DBObject) = {
        logger.info(s"$n")
        if (counter.incrementAndGet() % pageSize == 0) {
          logger.info(s"★ ★ ★ ★ ★ ★   Page:[$pageSize]  ★ ★ ★ ★ ★ ★ ")
          request(pageSize)
        }
      }
      override def onError(e: Throwable) = latch.countDown()
      override def onCompleted() = latch.countDown()
    }

    mongoQuery.stream[Observable](TEST_DB, ITEMS)
      .observeOn(RxExecutor)
      .subscribe(s)

    latch.await()
    counter.get() === MongoIntegrationEnv.itemsSize
  }

  "One to many join through stream with fixed columns" in new MongoStreamsEnviroment {
    initMongo()

    val buffer = mutable.Buffer.empty[String]
    val Sink = io.fillBuffer(buffer)
    implicit val cl = client

    val c = new CountDownLatch(1)
    var responses = new AtomicInteger(0)

    val mongoQuery = for { q ← "index" $gte 0 $lt 10 } yield q
    def qProgByLang(id: Int) = for { q ← "lang" $eq id } yield q

    val joinFlow = mongoQuery.stream[Observable](TEST_DB, LANGS).column[Int]("index")
      .innerJoin(qProgByLang(_).stream[Observable](TEST_DB, PROGRAMMERS).column[String]("name")) { (ind, p) ⇒ s"[lang:$ind/person:$p]" }

    val S = new Subscriber[String] {
      override def onStart() = request(1)
      override def onNext(n: String): Unit = {
        logger.info(s"receive $n")
        responses.incrementAndGet()
        request(1)
      }
      override def onError(e: Throwable): Unit = {
        logger.info(s"OnError: ${e.getMessage}")
        c.countDown()
      }
      override def onCompleted(): Unit = {
        logger.info("Interaction has been completed")
        c.countDown()
      }
    }

    joinFlow.observeOn(RxExecutor).subscribe(S)
    c.await()
    responses.get() === MongoIntegrationEnv.progSize
  }

  "One to many join through stream with raw objects" in new MongoStreamsEnviroment {
    initMongo()

    val buffer = mutable.Buffer.empty[String]
    val Sink = io.fillBuffer(buffer)
    implicit val cl = client

    val mongoQuery = for { q ← "index" $gte 0 $lt 10 } yield q
    def qProg(left: DBObject) = for { q ← "lang" $eq left.get("index").asInstanceOf[Int] } yield q

    val c = new CountDownLatch(1)
    val sb = new StringBuilder()
    var responses = new AtomicInteger(0)

    val S = new Subscriber[String] {
      override def onStart(): Unit = request(1)
      override def onNext(n: String): Unit = {
        logger.info(s"receive $n")
        sb.append(n).append("\t")
        responses.incrementAndGet()
        request(1)
      }
      override def onError(e: Throwable): Unit = {
        logger.info("OnError: " + e.getMessage)
        c.countDown()
      }
      override def onCompleted(): Unit = {
        logger.info("Interaction has been completed")
        c.countDown()
      }
    }

    val joinFlow = mongoQuery.stream[Observable](TEST_DB, LANGS).innerJoinRaw(qProg(_).stream[Observable](TEST_DB, PROGRAMMERS)) { (l, r) ⇒
      s"[lang:${l.get("name").asInstanceOf[String]}/person:${r.get("name").asInstanceOf[String]}]"
    }

    joinFlow.observeOn(RxExecutor).subscribe(S)
    c.await()
    responses.get() === MongoIntegrationEnv.progSize
  }
}