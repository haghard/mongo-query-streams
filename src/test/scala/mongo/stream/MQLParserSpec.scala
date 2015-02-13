package mongo.stream

import java.util.Arrays._
import com.mongodb.BasicDBObject
import mongo.parser.MqlParser
import org.specs2.mutable.Specification

class MQLParserSpec extends Specification {
  /*
  val inQ = """ { "num" : { "$in" : [ "3" , "4" , "5" , "67" , "8" , "9"] } } """
  val querySelector = """{ "age": { "$gt" : 40 } }"""
  val querySelector0 = """ { "name" : "Jack Boner" }"""
  val querySelector1 = """ { "name" : "Jack Boner", "age" : 57 }"""

  val multiCondQuerySelector = """{ age: { $gt: 39, $lt:42 }  }"""
  val multiFieldsQuerySelector = """{ "num" : { "$gt" : 3, "$lt": 90 }, "name" : { "$lt" : 56 } }"""

  val m0 = """{ "kkm":  {"$gt" : 3 } , { "$lte" : 67.5 } }"""
  val m = """{ "num" : {"$gt" : 3 , "$lt" : 90} , "name" : { "$ne" : false } }"""
  val m1 = """{ "kkm" : {"$gt" : 3 , "$lte" : 67.5 } }"""

  val simpleAND = """{ "$and": [ { "num": { "$gt": 0 } } , { "name"  : { "$lt" : 56 } } ] }"""

  val simpleAND1 = """{ "$and": [ { "num": { "$gt": 0 }  }, { "name": "56" } ] }  """

  val complexOR = """{ "$or": [ { "$and": [ { "num": { "$gt": 78, "$lt": 99 } }, { "name": "Jack99" } ] } , { "$and": [ { "num": { "$gt": 178 } }, { "name": "Jack11" } ] } ] } """

  val multiFieldsANDQuerySelector = """{ "$and": [ { "num" : { "$gt" : 3, "$lt": 90 } }, { "name": "Ivan" } ] }"""

  val line = """{ "num" : { "$gt" : "ISODate=2015-02-12T20:46:14+04:00" } }"""
  */

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