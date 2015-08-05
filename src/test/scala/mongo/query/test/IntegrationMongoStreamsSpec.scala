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

import com.mongodb.{ DBObject, MongoClient }
import de.bwaldvogel.mongo.MongoServer
import mongo.query.test.MongoIntegrationEnv._
import org.specs2.mutable.Specification
import scala.collection.mutable
import scalaz.\/-
import scalaz.concurrent.Task
import scalaz.stream.io

trait MongoStreamsEnviroment extends org.specs2.mutable.After {
  val logger = org.apache.log4j.Logger.getLogger("MongoStreams-Enviroment")
  var client: MongoClient = _
  var server: MongoServer = _

  def initMongo() = {
    val r = prepareMockMongo()
    client = r._1
    server = r._2
  }

  override def after = {
    logger.info("Close all resources")
    client.close()
    server.shutdown()
  }
}

class IntegrationMongoStreamsSpec extends Specification {
  import mongo._
  import dsl._
  import qb._
  import Interaction._
  import mongo.Order._
  import MongoIntegrationEnv._

  val P = scalaz.stream.Process

  "Build query and perform findOne" in new MongoStreamsEnviroment {
    initMongo()

    val p = for { q ← "index" $eq 0 } yield q
    val out = p.findOne(client, TEST_DB, LANGS).attemptRun

    out.isRight === true
    out.toOption.get.isRight === true
    val r = out.toOption.get.toOption.get
    r.get("index") === 0
  }

  "Build query and perform find batch" in new MongoStreamsEnviroment {
    initMongo()

    val p = for {
      _ ← "index" $gte 0 $lte 5
      _ ← "popularity_factor" $gte 0
      _ ← sort("popularity_factor" -> Ascending, "index" -> Descending)
      _ ← limit(4)
      q ← skip(2)
    } yield q

    val out = p.list(client, TEST_DB, LANGS).attemptRun
    out.isRight === true
    out.toOption.get.isRight === true
    val r = out.toOption.get.toOption.get
    r.get(BatchPrefix).asInstanceOf[java.util.List[DBObject]].size() === 3
  }

  "Build query and perform streaming using scalaz.Process" in new MongoStreamsEnviroment {
    initMongo()
    implicit val cl = client
    val q = for { ex ← "index" $gte 0 $lt 5 } yield ex

    val buf = mutable.Buffer.empty[DBObject]
    val sink = io.fillBuffer(buf)

    val out = (q.stream[MProcess](TEST_DB, LANGS) to sink).run.attemptRun
    out should be equalTo \/-(())
    langs.size === buf.size
  }

  "Build query and perform streaming using mongoStream" in new MongoStreamsEnviroment {
    initMongo()
    val field = "index"

    val query = for {
      _ ← field $gte 0 $lt 10
      q ← sort(field -> Descending)
    } yield q

    val buffer = mutable.Buffer.empty[String]
    val Sink = io.fillBuffer(buffer)

    val p = (for {
      element ← P.eval(Task.delay(client)).through(query.sChannel[MStream](TEST_DB, LANGS).column[String](field).out)
      _ ← element to Sink
    } yield ())

    p.onFailure(th ⇒ P.eval(Task.delay(logger.info(s"Exception: ${th.getMessage}"))))
      .onComplete(P.eval(Task.delay(logger.info(s"Interaction has been completed"))))
      .runLog.run

    langs.size === buffer.size
  }

  "One to many join through sChannel with fixed columns" in new MongoStreamsEnviroment {
    initMongo()
    val buffer = mutable.Buffer.empty[String]
    val Sink = io.fillBuffer(buffer)

    //Select all lang
    val qLang = for {
      _ ← "index" $gte 0 $lt 10
      q ← limit(5)
    } yield q

    //Select all programmers by specific lang
    def qProgByLang(id: Int) = for { q ← "lang" $eq id } yield q

    val left = qLang.sChannel[MStream](TEST_DB, LANGS).column[Int]("index")
    val right: Int ⇒ mongo.query.DBChannel[MongoClient, String] =
      id ⇒
        qProgByLang(id).sChannel[MStream](TEST_DB, PROGRAMMERS).column[String]("name")

    (for {
      element ← P.eval(Task.delay(client)) through ((left innerJoin right) { (i, p) ⇒ s"[lang:$i/person:$p]" }.out)
      _ ← element to Sink
    } yield ())
      .onFailure(th ⇒ P.eval(Task.delay(logger.info(s"Exception: ${th.getMessage}"))))
      .onComplete(P.eval(Task.delay(logger.info(s"Interaction has been completed"))))
      .runLog.run

    logger.info(buffer)
    buffer.size === 10
  }

  "One to many join through sChannel with raw objects" in new MongoStreamsEnviroment {
    initMongo()
    val buffer = mutable.Buffer.empty[String]
    val Sink = io.fillBuffer(buffer)
    implicit val cl = client

    val qLang = for { q ← "index" $gte 0 $lt 10 } yield q
    def qProg(left: DBObject) = for { q ← "lang" $eq left.get("index").asInstanceOf[Int] } yield q

    val query = qLang.sChannel[MStream](TEST_DB, LANGS)
      .innerJoinRaw(qProg(_).sChannel[MStream](TEST_DB, PROGRAMMERS)) { (l, r) ⇒
        s"[lang:${l.get("name").asInstanceOf[String]}/person:${r.get("name").asInstanceOf[String]}]"
      }

    val p = for {
      element ← P.eval(Task.delay(client)) through query.out
      _ ← element to Sink
    } yield ()

    p.onFailure(th ⇒ P.eval(Task.delay(logger.info(s"Exception: ${th.getMessage}"))))
      .onComplete(P.eval(Task.delay(logger.info(s"Interaction has been completed"))))
      .runLog.run

    logger.info(buffer)
    buffer.size === 10
  }
}