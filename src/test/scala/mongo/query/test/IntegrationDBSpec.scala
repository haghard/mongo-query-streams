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

import mongo.query.create

import mongo._
import querydsl._

import com.mongodb.DBObject
import org.specs2.mutable._
import org.apache.log4j.Logger
import scala.collection.JavaConversions._
import scalaz.stream.Process._
import scala.collection.mutable._

//https://twitter.github.io/scala_school/ru/specs.html
trait Enviroment[T] extends org.specs2.mutable.Before {
  import mongo.query.test.MongoIntegrationEnv.{ mockDB, sinkWithBuffer, executor }

  protected lazy val Resource = mockDB()

  protected val (sink, buffer) = sinkWithBuffer[T]

  override def before = Resource
}

class IntegrationMongoDBSpec extends Specification {
  import MongoIntegrationEnv._
  private val logger = Logger.getLogger(classOf[IntegrationMongoDBSpec])

  private def producers(id: Int) =
    create { b ⇒
      b.q("producer_num" $eq id)
      b.collection(PRODUCER)
      b.db(TEST_DB)
      b.readPreference(ReadPreference.Nearest)
    }.column[String]("name")

  private def categories(obj: DBObject) = {
    val ids = asScalaBuffer(obj.get("categories").asInstanceOf[java.util.List[Int]])
    create { b ⇒
      b.q("category" $in ids)
      b.sort("name" $eq -1)
      b.collection(CATEGORY)
      b.db(TEST_DB)
    }
  }

  "Hit server with multiple predicates query" in new Enviroment[Int] {
    val products = create { b ⇒
      b.q("article" $gt 2 $lt 40)
      b.collection(PRODUCT)
      b.db(TEST_DB)
      b.readPreference(ReadPreference.Nearest)
      b.limit(5)
    }.column[Int]("article")

    val program = for {
      dbObject ← Resource through products.out
      _ ← dbObject to sink
    } yield ()

    program.runLog.run
    buffer must be equalTo ArrayBuffer(3, 35)
  }

  "Hit server with sorted query with folding results in single value" in new Enviroment[String] {
    val products = create { b ⇒
      b.q("article" $gt 2 $lt 40)
      b.sort("article" $eq -1)
      b.collection(PRODUCT)
      b.db(TEST_DB)
    }.column[Int]("article").map(_.toString)

    val p = for {
      dbObject ← Resource through products.out
      _ ← dbObject.fold1 { _ + "-" + _ } to sink
    } yield ()

    p.runLog.run
    buffer(0) must be equalTo "35-3"
  }

  "Hit server with native json query" in new Enviroment[String] {
    val products = create { b ⇒
      import b._
      q("""{ "article" : 1 } """)
      collection(PRODUCT)
      b.db(TEST_DB)
    }.column[String]("name")

    val p = for {
      dbObject ← Resource through products.out
      _ ← dbObject to sink
    } yield ()

    p.runLog.run
    buffer(0) must be equalTo "Extra Large Wheel Barrow"
  }

  "Hit server with join ONE-TO-MANY" in new Enviroment[String] {
    val pNums = create { b ⇒
      b.q(""" { "article": 1 } """)
      b.collection(PRODUCT)
      b.db(TEST_DB)
    }.column[Int]("producer_num") //|> asNum

    val p = for {
      dbObject ← Resource through (for {
        n ← pNums
        prod ← producers(n)
      } yield (prod)).out
      _ ← dbObject to sink
    } yield ()

    p.runLog.run
    buffer must be equalTo ArrayBuffer("Puma", "Reebok")
  }

  "Hit server with join ONE-TO-MANY with fold in single value" in new Enviroment[String] {
    val qProds = create { b ⇒
      b.q(Obj("article" -> 1).toString)
      b.collection(PRODUCT)
      b.db(TEST_DB)
    }

    val p = for {
      dbObject ← Resource.through(qProds.innerJoinRaw(categories(_)) { (l, r) ⇒
        s"${l.get("name")} - ${r.get("name").asInstanceOf[String]}"
      }.out)
      _ ← dbObject.fold1 {
        _ + "/" + _
      } to sink
    } yield ()

    p.runLog.run
    buffer(0) must be equalTo
      "Extra Large Wheel Barrow - Rubberized Work Glove/Extra Large Wheel Barrow - Gardening Tools/Extra Large Wheel Barrow - Car Tools"
  }

  "Hit server with join ONE-TO-MANY with monoid" in new Enviroment[String] {
    import scalaz._
    import Scalaz._
    implicit val M = scalaz.Monoid[String]

    val prodsWithCategoryIds = create { b ⇒
      b.q(Obj("article" -> Obj(($in(), List(1, 2)))).toString)
      b.collection(PRODUCT)
      b.db(TEST_DB)
    }

    val p = for {
      dbObject ← Resource through (prodsWithCategoryIds.innerJoinRaw(categories(_))((l, r) ⇒ r.get("name").asInstanceOf[String]).out)
      _ ← dbObject.foldMap(_ + ", ") to sink
    } yield ()

    p.runLog.run
    logger.debug(buffer(0))
    buffer(0) must be equalTo "Rubberized Work Glove, Gardening Tools, Car Tools, Gardening Tools, Car1 Tools, "
  }

  "Use pipe and separate transducer for parse result " in new Enviroment[Int] {
    val products = create { b ⇒
      b.q(""" { "article": { "$gt" : 0, "$lt" : 4 } }""")
      b.collection(PRODUCT)
      b.db(TEST_DB)
      b.limit(2)
      b.skip(1)
    }.column[Int]("article")

    val p = for {
      dbObject ← Resource through products.out
      _ ← dbObject to sink
    } yield ()

    p.runLog.run
    buffer must be equalTo ArrayBuffer(2, 3)
  }

  "Hit server with zip. Deterministic merge 2 streams in single value" in new Enviroment[String] {
    val products = create { b ⇒
      b.q("article" $eq 1)
      b.collection(PRODUCT)
      b.db(TEST_DB)
    }.column[Int]("article")

    val categories = create { b ⇒
      b.q("category" $eq 12)
      b.collection(CATEGORY)
      b.db(TEST_DB)
    }.column[Int]("category")

    val p = for {
      dbObject ← Resource through (products zip categories)
        .map { r: (Int, Int) ⇒ r._1 + "-" + r._2 }.out
      _ ← dbObject to sink
    } yield ()

    p.runLog.run
    buffer(0) must be equalTo "1-12"
  }

  "Hit server with zipWith. Deterministic merge 2 streams in single value with function" in new Enviroment[String] {
    //if one process halts, so second process will halt too
    implicit val dataExtracter = { (a: DBObject, b: DBObject) ⇒
      a.get("article").asInstanceOf[Int] + "-" +
        b.get("category").asInstanceOf[Int]
    }

    val products = create { b ⇒
      b.q("article" $in Seq(1, 2, 3))
      b.collection(PRODUCT)
      b.db(TEST_DB)
    }

    val categories = create { b ⇒
      b.q("category" $in Seq(12, 13))
      b.collection(CATEGORY)
      b.db(TEST_DB)
    }

    val tee = products zipWith categories

    val p = for {
      dbObject ← Resource through tee.out
      _ ← dbObject to sink
    } yield ()

    p.runLog.run
    buffer must be equalTo ArrayBuffer("1-12", "2-13")
  }
}
