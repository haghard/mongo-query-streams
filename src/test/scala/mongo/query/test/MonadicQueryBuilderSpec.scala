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

import mongo.dsl._
import java.util.Arrays._
import com.mongodb.BasicDBObject
import mongo.mqlparser.MqlParser
import org.specs2.mutable.Specification

class MonadicQueryBuilderSpec extends Specification {

  import free._

  "monadic query" should {
    "be parsed" in {
      val program = for {
        _ ← "article" $gt 0 $lt 6 $nin Seq(4, 5)
        x ← "producer_num" $eq 1
      } yield x

      val actual = MqlParser().parse(program.toQueryStr)

      val expected = new BasicDBObject("article",
        new BasicDBObject("$gt", 0).append("$lt", 6).append("$nin", asList(4, 5)))
        .append("producer_num", 1)

      actual must be equalTo expected
      program.toQuery must be equalTo expected
    }
  }

  "monadic query" should {
    "be parsed 2" in {
      val program = for {
        _ ← "producer_num" $eq 1
        x ← "article" $gt 0 $lt 6 $nin Seq(4, 5)
      } yield x

      val actual = MqlParser().parse(program.toQueryStr)

      val expected = new BasicDBObject("producer_num", 1).append("article",
        new BasicDBObject("$gt", 0).append("$lt", 6).append("$nin", asList(4, 5)))

      actual must be equalTo expected
      program.toQuery must be equalTo expected
    }
  }
}
