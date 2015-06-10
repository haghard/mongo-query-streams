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
}