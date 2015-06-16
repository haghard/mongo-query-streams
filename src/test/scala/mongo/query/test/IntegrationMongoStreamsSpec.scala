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

import com.mongodb.{ BasicDBObject, DBObject, MongoClient }
import de.bwaldvogel.mongo.MongoServer
import mongo.dsl3._
import mongo.query.test.MongoIntegrationEnv._
import org.specs2.mutable.Specification
import scala.collection.mutable.{ ArrayBuffer, Buffer }
import scalaz.\/-
import scalaz.concurrent.Task
import scalaz.stream.io

trait MongoStreamsEnviroment extends org.specs2.mutable.After {
  val logger = org.apache.log4j.Logger.getLogger("MongoStreams-Enviroment")
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

class IntegrationMongoStreamsSpec extends Specification {
  import mongo._
  import dsl3._
  import Query._
  import Interaction._
  import MongoIntegrationEnv._
  import StreamerFactory._
  import scalaz.stream.Process

  "Build query and perform findOne" in new MongoStreamsEnviroment {
    initMongo

    val p = for { q ← "index" $eq 0 } yield q
    val out = p.one(client, DB_NAME, LANGS).attemptRun

    out.isRight === true
    out.toOption.get.isRight === true
    val r = out.toOption.get.toOption.get
    r.get("index") === 0
  }

  "Build query and perform find batch" in new MongoStreamsEnviroment {
    initMongo

    val p = for { q ← "index" $gte 1 $lt 3 } yield q

    val out = p.list(client, DB_NAME, LANGS).attemptRun
    out.isRight === true
    out.toOption.get.isRight === true
    val r = out.toOption.get.toOption.get
    r.get(BatchPrefix).asInstanceOf[java.util.List[DBObject]].size() === 2
  }

  "Build query and perform streaming using scalaz.Process" in new MongoStreamsEnviroment {
    initMongo
    implicit val cl = client
    val q = for { ex ← "index" $gte 0 $lt 5 } yield ex

    val buf = Buffer.empty[BasicDBObject]
    val sink = io.fillBuffer(buf)

    val out = (q.stream[SProc](DB_NAME, LANGS) to sink).run.attemptRun
    out should be equalTo \/-(())
    langs.size === buf.size
  }

  "Build query and perform streaming using mongoStream" in new MongoStreamsEnviroment {
    initMongo
    implicit val cl = client
    val field = "index"
    val query = for { ex ← field $gte 0 $lt 10 } yield ex

    val buffer = Buffer.empty[String]
    val Sink = io.fillBuffer(buffer)

    val p = (for {
      element ← Process.eval(Task.delay(client))
        .through(query.streamC[MStream](DB_NAME, LANGS).column[String](field).out)
      _ ← element to Sink
    } yield ())

    p.onFailure(th ⇒ Process.eval(Task.delay(logger.info(s"Exception: ${th.getMessage}"))))
      .onComplete(Process.eval(Task.delay(logger.info(s"Interaction has been completed"))))
      .runLog.run

    langs.size === buffer.size
  }

  "One to many join through mongoStream with fixed columns" in new MongoStreamsEnviroment {
    initMongo
    val buffer = Buffer.empty[String]
    val Sink = io.fillBuffer(buffer)
    implicit val cl = client

    //Select all lang
    val qLang = for { q ← "index" $gte 0 $lt 10 } yield q

    //Select all programmers by specific lang
    def qProgByLang(id: Int) = for { q ← "lang" $eq id } yield q

    val query = qLang.streamC[MStream](DB_NAME, LANGS).column[Int]("index")
      .innerJoin(qProgByLang(_).streamC[MStream](DB_NAME, PROGRAMMERS).column[String]("name")) { (ind, p) ⇒ s"[lang:$ind/person:$p]" }

    val p = for {
      element ← Process.eval(Task.delay(client)) through query.out
      _ ← element to Sink
    } yield ()

    p.onFailure(th ⇒ Process.eval(Task.delay(logger.info(s"Exception: ${th.getMessage}"))))
      .onComplete(Process.eval(Task.delay(logger.info(s"Interaction has been completed"))))
      .runLog.run

    logger.info(buffer)
    buffer.size === 10
  }

  "One to many join through mongoStream with raw objects" in new MongoStreamsEnviroment {
    initMongo
    val buffer = Buffer.empty[String]
    val Sink = io.fillBuffer(buffer)
    implicit val cl = client

    val qLang = for { q ← "index" $gte 0 $lt 10 } yield q
    def qProg(left: BasicDBObject) = for { q ← "lang" $eq left.get("index").asInstanceOf[Int] } yield q

    val query = qLang.streamC[MStream](DB_NAME, LANGS).innerJoinRaw(qProg(_).streamC[MStream](DB_NAME, PROGRAMMERS)) { (l, r) ⇒
      s"[lang:${l.get("name").asInstanceOf[String]}/person:${r.get("name").asInstanceOf[String]}]"
    }

    val p = for {
      element ← Process.eval(Task.delay(client)) through query.out
      _ ← element to Sink
    } yield ()

    p.onFailure(th ⇒ Process.eval(Task.delay(logger.info(s"Exception: ${th.getMessage}"))))
      .onComplete(Process.eval(Task.delay(logger.info(s"Interaction has been completed"))))
      .runLog.run

    logger.info(buffer)
    buffer.size === 10
  }
}