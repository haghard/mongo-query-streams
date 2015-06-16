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

import com.mongodb.BasicDBObject
import mongo.query.test.{ MongoIntegrationEnv, MongoStreamsEnviroment }
import org.specs2.mutable.Specification
import rx.lang.scala.{ Observable, Subscriber }

import scala.collection.mutable.Buffer
import scalaz.concurrent.Task
import scalaz.stream.io

class ObservableSpec extends Specification {
  import mongo._
  import dsl3._
  import Interaction._
  import MongoIntegrationEnv._
  import Query._
  /*

  "Build query and perform streaming using Observable" in new MongoStreamsEnviroment {
    initMongo

    val ExpectedSize = 5
    val batchSize = 3
    var responses = new AtomicInteger(0)
    val c = new CountDownLatch(1)
    val q = for { ex ← "index" $gte 0 $lt 10 } yield ex

    val s = new Subscriber[BasicDBObject] {
      override def onStart(): Unit = request(batchSize)
      override def onNext(n: BasicDBObject): Unit = {
        logger.info(s"receive $n")
        responses.incrementAndGet()
        if (responses.get() % batchSize == 0) {
          request(batchSize)
        }
      }
      override def onError(e: Throwable): Unit = {
        logger.info("OnError: " + e.getMessage)
        c.countDown()
      }
      override def onCompleted(): Unit = {
        logger.info("OnCompile")
        c.countDown()
      }
    }

    Task(q.stream[Observable](client, DB_NAME, LANGS).subscribe(s))(executor) runAsync (_ ⇒ ())
    c.await()
    ExpectedSize === responses.get()
  }
*/

  "One to many join through mongoStream with fixed columns" in new MongoStreamsEnviroment {
    initMongo
    val buffer = Buffer.empty[String]
    val Sink = io.fillBuffer(buffer)
    implicit val cl = client

    val c = new CountDownLatch(1)
    var responses = new AtomicInteger(0)

    val qLang = for { q ← "index" $gte 0 $lt 10 } yield q
    def qProgByLang(id: Int) = for { q ← "lang" $eq id } yield q

    val query = qLang.streamC[Observable](TEST_DB, LANGS).column[Int]("index")
      .leftJoin(qProgByLang(_).streamC[Observable](TEST_DB, PROGRAMMERS).column[String]("name")) { (ind, p) ⇒ s"[lang:$ind/person:$p]" }

    val s = new Subscriber[String] {
      override def onStart(): Unit = request(1)
      override def onNext(n: String): Unit = {
        logger.info(s"receive $n")
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

    Task(query.subscribe(s)) runAsync (_ ⇒ ())
    c.await()
    10 === responses.get()
  }
  /*
  "One to many join through mongoStream with raw objects" in new MongoStreamsEnviroment {
    initMongo
    val buffer = Buffer.empty[String]
    val Sink = io.fillBuffer(buffer)
    implicit val cl = client

    val qLang = for { q ← "index" $gte 0 $lt 10 } yield q
    def qProg(left: BasicDBObject) = for { q ← "lang" $eq left.get("index").asInstanceOf[Int] } yield q

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

    val query = qLang.stream2[Observable](DB_NAME, LANGS).joinRaw(qProg(_).stream2[Observable](DB_NAME, PROGRAMMERS)) { (l, r) ⇒
      s"[lang:${l.get("name").asInstanceOf[String]}/person:${r.get("name").asInstanceOf[String]}]"
    }

    Task(query.subscribe(s)) runAsync (_ ⇒ ())
    c.await()
    logger.info(sb.toString())
    10 === responses.get()
  }*/
}
