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
import java.util.concurrent.atomic.AtomicInteger

import com.mongodb.{ BasicDBObject, DBObject, MongoClient }
import de.bwaldvogel.mongo.MongoServer
import mongo.query.test.MongoIntegrationEnv._
import org.specs2.mutable.Specification

import scala.collection.mutable.{ ArrayBuffer, Buffer }
import scalaz.\/-
import scalaz.concurrent.Task
import scalaz.stream.io

trait Dsl3Enviroment extends org.specs2.mutable.After {
  val logger = org.apache.log4j.Logger.getLogger("DSL3-Enviroment")
  var client: MongoClient = _
  var server: MongoServer = _

  def initMongo = {
    val r = prepareMockMongo()
    client = r._1
    server = r._2
  }

  override def after = {
    logger.info("Close all resources")
    client.close
    server.shutdown
  }

  def EnvLogger(): scalaz.stream.Sink[Task, String] = MongoIntegrationEnv.LoggerSink(logger)
}

class IntegrationMongoDsl3Spec extends Specification {
  import mongo._
  import dsl3._
  import Query._
  import Interaction._
  import MongoIntegrationEnv._
  import StreamerFactory._
  import scalaz.stream.Process

  "Build query and perform findOne" in new Dsl3Enviroment {
    initMongo

    val p = for {
      _ ← "producer_num" $eq 1
      q ← "article" $gt 0 $lt 6 $nin Seq(4, 5)
    } yield q

    val out = p.one(client, DB_NAME, PRODUCT).attemptRun

    out.isRight === true
    out.toOption.get.isRight === true
    val r = out.toOption.get.toOption.get
    r.get("article") === 1
    r.get("producer_num") === 1
  }

  "Build query and perform find batch" in new Dsl3Enviroment {
    initMongo

    val p = for { q ← "producer_num" $gte 1 $lt 3 } yield q

    val out = p.list(client, DB_NAME, PRODUCT).attemptRun
    out.isRight === true
    out.toOption.get.isRight === true
    val r = out.toOption.get.toOption.get
    r.get(BatchPrefix).asInstanceOf[java.util.List[DBObject]].size() === 2
  }

  val ExpectedSize = 11

  "Build query and perform streaming using scalaz.Process" in new Dsl3Enviroment {
    initMongo
    val q = for { ex ← "value" $gte 0 $lt 11 } yield ex

    val buf = Buffer.empty[BasicDBObject]
    val sink = io.fillBuffer(buf)

    val out = (q.stream[ScalazProcess](client, DB_NAME, STREAMS) to sink).run.attemptRun
    out should be equalTo \/-(())
    ExpectedSize === buf.size
  }

  "Build query and perform streaming using Observable" in new Dsl3Enviroment {

    import rx.lang.scala.Observable
    import rx.lang.scala.Subscriber
    initMongo

    val batchSize = 3
    var responses = new AtomicInteger(0)
    val c = new CountDownLatch(1)

    val q = for { ex ← "value" $gte 0 $lt 11 } yield ex

    //Add more records for steaming example
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

    Task(q.stream[Observable](client, DB_NAME, STREAMS).subscribe(s))(executor) runAsync (_ ⇒ ())

    c.await()
    responses.get() === ExpectedSize
  }

  "Build query and perform streaming using mongoStream for compose" in new Dsl3Enviroment {
    initMongo

    val field = "value"
    val query = for { ex ← field $gte 0 $lt 11 } yield ex

    val buffer = Buffer.empty[Int]
    val Sink = io.fillBuffer(buffer)

    val p = (for {
      element ← Process.eval(Task.delay(client))
        .through(query.mongoStream(DB_NAME, STREAMS).column[Int](field).channel)
      _ ← element to Sink
    } yield ())

    p.onFailure(th ⇒ Process.eval(Task.delay(logger.info(s"Exception: ${th.getMessage}"))))
      .onComplete(Process.eval(Task.delay(logger.info(s"Interaction has been completed"))))
      .runLog.run

    buffer.size === 11
  }

  "One to many with mongoStream" in new Dsl3Enviroment {
    initMongo

    val buffer = Buffer.empty[String]
    val Sink = io.fillBuffer(buffer)

    val q = for { q ← "article" $eq 1 } yield q
    def nested(id: Int) = for { q ← "producer_num" $eq id } yield q

    val p = for {
      element ← Process.eval(Task.delay(client)) through (for {
        n ← q.mongoStream(DB_NAME, PRODUCT).column[Int]("producer_num")
        p ← nested(n).mongoStream(DB_NAME, PRODUCER).column[String]("name")
      } yield p).channel
      _ ← element to Sink
    } yield ()

    p.onFailure(th ⇒ Process.eval(Task.delay(logger.info(s"Exception: ${th.getMessage}"))))
      .onComplete(Process.eval(Task.delay(logger.info(s"Interaction has been completed"))))
      .runLog.run

    buffer.size === 2
    buffer === ArrayBuffer("Puma", "Reebok")
  }
}