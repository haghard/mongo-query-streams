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

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.CountDownLatch

import com.mongodb.DBObject
import mongo.query.test.{ MongoIntegrationEnv, MongoStreamsEnviroment }
import org.specs2.mutable.Specification
import rx.lang.scala.schedulers.ExecutionContextScheduler
import rx.lang.scala.{ Observable, Subscriber }

import scala.collection.mutable.Buffer
import scala.concurrent.ExecutionContext
import scalaz.stream.io

class ObservableSpec extends Specification {
  import mongo._
  import mongo.join.observable._
  import dsl._
  import qb._
  import MongoIntegrationEnv._

  "Build query and perform streaming using Observable" in new MongoStreamsEnviroment {
    initMongo

    val batchSize = 3
    var responses = new AtomicInteger(0)

    implicit val cl = client
    val c = new CountDownLatch(1)
    val q = for { ex ← "index" $gte 0 $lt 10 } yield ex

    val s = new Subscriber[DBObject] {
      override def onStart(): Unit = request(batchSize)
      override def onNext(n: DBObject): Unit = {
        logger.info(s"receive $n")
        if (responses.incrementAndGet() % batchSize == 0)
          request(batchSize)
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

    q.stream[Observable](TEST_DB, LANGS)
      .observeOn(ExecutionContextScheduler(ExecutionContext.fromExecutor(executor)))
      .subscribe(s)
    c.await()
    5 === responses.get()
  }

  "One to many join through mongoStream with fixed columns" in new MongoStreamsEnviroment {
    initMongo
    val buffer = Buffer.empty[String]
    val Sink = io.fillBuffer(buffer)
    implicit val cl = client

    val c = new CountDownLatch(1)
    var responses = new AtomicInteger(0)

    val qLang = for { q ← "index" $gte 0 $lt 10 } yield q
    def qProgByLang(id: Int) = for { q ← "lang" $eq id } yield q

    val query = qLang.stream[Observable](TEST_DB, LANGS).column[Int]("index")
      .innerJoin(qProgByLang(_).stream[Observable](TEST_DB, PROGRAMMERS).column[String]("name")) { (ind, p) ⇒ s"[lang:$ind/person:$p]" }

    val s = new Subscriber[String] {
      override def onStart(): Unit = request(1)
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

    query.observeOn(ExecutionContextScheduler(ExecutionContext.fromExecutor(executor)))
      .subscribe(s)
    c.await()
    10 === responses.get()
  }

  "One to many join through mongoStream with raw objects" in new MongoStreamsEnviroment {
    initMongo
    val buffer = Buffer.empty[String]
    val Sink = io.fillBuffer(buffer)
    implicit val cl = client

    val qLang = for { q ← "index" $gte 0 $lt 10 } yield q
    def qProg(left: DBObject) = for { q ← "lang" $eq left.get("index").asInstanceOf[Int] } yield q

    val c = new CountDownLatch(1)
    val sb = new StringBuilder()
    var responses = new AtomicInteger(0)

    val s = new Subscriber[String] {
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

    val query = qLang.stream[Observable](TEST_DB, LANGS).innerJoinRaw(qProg(_).stream[Observable](TEST_DB, PROGRAMMERS)) { (l, r) ⇒
      s"[lang:${l.get("name").asInstanceOf[String]}/person:${r.get("name").asInstanceOf[String]}]"
    }

    query.observeOn(ExecutionContextScheduler(ExecutionContext.fromExecutor(executor)))
      .subscribe(s)
    c.await()
    10 === responses.get()
  }
}