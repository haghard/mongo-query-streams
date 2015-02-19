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

import java.util.Arrays._
import com.mongodb.BasicDBObject
import mongo.mqlparser.MqlParser
import org.specs2.mutable.Specification

class MQLParserSpec extends Specification {

  "Single selector query" should {
    "be parsed" in {
      val dt = "13 Feb 2015 10:34:47:273 PM MSK"
      val line = """{ "dt" : { "$gt" : """ + "\"" + dt + "\" } }"
      MqlParser().parse(line) must beEqualTo(new BasicDBObject("dt",
        new BasicDBObject("$gt", mongo.formatter().parse(dt))))
    }
  }

  "Single selector query with $gt" should {
    "be parsed" in {
      val expected = new BasicDBObject("num", new BasicDBObject("$gt", 3))
      val actual = MqlParser().parse(expected.toString)
      actual mustEqual expected
    }
  }

  "Single selector query with $gte" should {
    "be parsed" in {
      val expected = new BasicDBObject("num", new BasicDBObject("$gte", 3))
      val actual = MqlParser().parse(expected.toString)
      actual mustEqual expected
    }
  }

  "Single selector query with $lt" should {
    "be parsed" in {
      val expected = new BasicDBObject("num", new BasicDBObject("$lt", 3))
      val actual = MqlParser().parse(expected.toString)
      actual mustEqual expected
    }
  }

  "Single selector query with $lte" should {
    "be parsed" in {
      val expected = new BasicDBObject("num", new BasicDBObject("$lte", 3))
      val actual = MqlParser().parse(expected.toString)
      actual mustEqual expected
    }
  }

  "Single selector query with $ne" should {
    "be parsed" in {
      val expected = new BasicDBObject("num", new BasicDBObject("$ne", 3))
      val actual = MqlParser().parse(expected.toString)
      actual mustEqual expected
    }
  }

  "Single selector query with $in" should {
    "be parsed" in {
      val expected =
        new BasicDBObject("num", new BasicDBObject("$in", asList(1, 2, 3)))
      val actual = MqlParser().parse(expected.toString)
      actual mustEqual expected
    }
  }

  "Single selector query with $all" should {
    "be parsed" in {
      val expected =
        new BasicDBObject("num", new BasicDBObject("$all", asList(1, 2, 3)))
      val actual = MqlParser().parse(expected.toString)
      actual mustEqual expected
    }
  }

  "Single selector query with $nin" should {
    "be parsed" in {
      val expected =
        new BasicDBObject("num", new BasicDBObject("$nin", asList(1, 2, 3)))
      val actual = MqlParser().parse(expected.toString)
      actual mustEqual expected
    }
  }

  "Single selector with nested conditions" should {
    "be parsed" in {
      val expected =
        new BasicDBObject("num", new BasicDBObject("$gt", 3)
          .append("$lt", 20).append("$nin", asList(11, 12)))
      val actual = MqlParser().parse(expected.toString)
      actual mustEqual expected
    }
  }

  "Multi-conditional and multi-nested conditions" should {
    "be parsed" in {
      val expected = new BasicDBObject("num",
        new BasicDBObject("$gt", 3).append("$lt", 90)).append("name",
        new BasicDBObject("$ne", false))

      val actual = MqlParser().parse(expected.toString)
      expected mustEqual actual
    }
  }

  "Logical AND query" should {
    "be parsed" in {
      val expected = new BasicDBObject("$and",
        asList(
          new BasicDBObject("num0", new BasicDBObject("$gt", 3)),
          new BasicDBObject("num1", new BasicDBObject("$lt", 36))))
      val actual = MqlParser().parse(expected.toString)
      expected mustEqual actual
    }
  }

  "Logical OR query" should {
    "be parsed" in {
      val expected = new BasicDBObject("$or",
        asList(
          new BasicDBObject("num0", new BasicDBObject("$gt", 3)),
          new BasicDBObject("num1", new BasicDBObject("$lt", 36))))
      val actual = MqlParser().parse(expected.toString)
      expected mustEqual actual
    }
  }

  "Logical AND query with complex inner queries" should {
    "be parsed" in {
      val expected = new BasicDBObject("$and",
        asList(
          new BasicDBObject("num", new BasicDBObject("$gte", 3).append("$lt", 10)),
          new BasicDBObject("name", "Jack Bauer")))
      val actual = MqlParser().parse(expected.toString)
      expected mustEqual actual
    }
  }

  "Logical OR query with nested AND's" should {
    "be parsed" in {
      val left = new BasicDBObject("$and",
        asList(
          new BasicDBObject("num", new BasicDBObject("$gt", 78).append("$lt", 99)),
          new BasicDBObject("name", "Jack Bauer")))

      val right = new BasicDBObject("$and",
        asList(
          new BasicDBObject("num", new BasicDBObject("$gt", 78).append("$lt", 99)),
          new BasicDBObject("name", "James Bond")))

      val expected = new BasicDBObject("$or", asList(left, right))

      val actual = MqlParser().parse(expected.toString)
      expected mustEqual actual
    }
  }
}
