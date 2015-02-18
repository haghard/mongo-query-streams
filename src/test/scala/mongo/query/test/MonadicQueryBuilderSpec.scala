package mongo.query.test

import com.mongodb.BasicDBObject
import mongo.mqlparser.MqlParser
import mongo.{ $ne, $nin, $lt, $gt, $eq }
import org.specs2.mutable.Specification
import mongo.dsl._
import mongo.dsl2._
import free._

class MonadicQueryBuilderSpec extends Specification {

  "Single selector query" should {
    "be parsed" in {
      val program = for {
        _ ← "article" $gt 0 $lt 6 $nin Seq(4,5)
        x ← "producer_num" $eq 1
      } yield x

      val actual = MqlParser().parse(instructions(program))

      val expected = new BasicDBObject("article", new BasicDBObject("$gt", 0).append("$lt", 3))
        .append("producer_num", new BasicDBObject("$eq", 1))

      actual must be equalTo expected
    }
  }
}
