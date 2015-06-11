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

import scala.collection.mutable.Buffer
import scalaz.\/-
import scalaz.concurrent.Task
import scalaz.stream.{ io, Process }

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
}

class IntegrationMongoDsl3Spec extends Specification {
  import mongo._
  import dsl3._
  import Query._
  import Interaction._
  import MongoIntegrationEnv._
  import StreamerFactory._

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

  "Build query and perform streaming using scalaz.Process" in new Dsl3Enviroment {
    initMongo
    val Records = 2
    val q = for { ex ← "producer_num" $gte 1 $lt 5 } yield ex

    val buf = Buffer.empty[BasicDBObject]
    val sink = io.fillBuffer(buf)

    val out = (q.stream[({ type λ[x] = Process[Task, x] })#λ](client, DB_NAME, PRODUCT)
      to sink).run.attemptRun

    out should be equalTo \/-(())
    Records === buf.size
  }

  "Build query and perform streaming using Observable" in new Dsl3Enviroment {
    import rx.lang.scala.Observable
    import rx.lang.scala.Subscriber
    initMongo

    val Records = 2
    var responses = new AtomicInteger(0)
    val c = new CountDownLatch(1)

    val q = for { ex ← "producer_num" $gte 1 $lt 5 } yield ex

    val s = new Subscriber[BasicDBObject] {
      override def onStart(): Unit = request(1)
      override def onNext(n: BasicDBObject): Unit = {
        logger.info(s"receive $n")
        responses.incrementAndGet()
        request(1)
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

    Task(q.stream[Observable](client, DB_NAME, PRODUCT).subscribe(s))(executor) runAsync (_ ⇒ ())

    c.await()
    responses.get() === Records
  }
}