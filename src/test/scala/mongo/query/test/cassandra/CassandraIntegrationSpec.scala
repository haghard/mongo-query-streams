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

package mongo.query.test.cassandra

import scalaz.concurrent.Task
import java.net.InetSocketAddress
import scalaz.stream.{ Process, io }
import scala.collection.mutable.Buffer
import cassandra.CassandraProcessStream
import com.datastax.driver.core.Cluster
import scala.collection.JavaConverters._
import com.datastax.driver.core.{ Row ⇒ CRow }
import org.scalatest.{ Matchers, WordSpecLike }

class CassandraIntegrationSpec extends WordSpecLike with Matchers with CassandraEnviromentLifecycle {
  import mongo._
  import join._
  import dsl._
  import qb._

  "JoinG with CassandraProcess" should {
    "have run" in {
      val P = Process
      val buffer = Buffer.empty[String]
      val Sink = io.fillBuffer(buffer)
      val cassandraHost = List(new InetSocketAddress("127.0.0.1", 9142)).asJava
      implicit val client = Cluster.builder().addContactPointsWithPorts(cassandraHost).build

      val qLang = for { q ← qFreeM("SELECT id, name FROM {0}") } yield q

      def qProg(r: CRow) = for {
        _ ← qFreeM("SELECT * FROM {0} WHERE lang = ? allow filtering")
        q ← cParam[java.lang.Long]("id_lang", r.getLong("id"))
      } yield q

      val query = JoinG[CassandraProcessStream].join(qLang, LANGS, qProg, PROGRAMMERS, "world") { (l, r) ⇒
        s"Pk: ${l.getLong("id")} lang: ${l.getString("name")} name: ${r.getString(2)}"
      }

      (for {
        row ← P.eval(Task.delay(client)) through query.out
        _ ← row to Sink
      } yield ())
        .onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); P.halt }
        .onComplete { P.eval(Task.delay(logger.debug("Interaction has been completed"))) }
        .runLog.run

      logger.info("********************** Result:" + buffer)
      buffer.size === 10
    }
  }
}
