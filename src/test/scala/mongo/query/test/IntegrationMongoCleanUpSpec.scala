package mongo.query.test

import com.mongodb._
import mongo.query.query

import org.specs2.mutable._
import mongo.dsl._
import mongo.dsl2._
import org.apache.log4j.Logger
import scalaz.stream.Process._
import scala.collection.mutable._
import scalaz.stream.process1

//https://twitter.github.io/scala_school/ru/specs.html
trait Enviroment[T] extends org.specs2.mutable.Before {
  import mongo.query.test.MongoIntegrationEnv.{ mockDB, sinkWithBuffer, executor }

  protected lazy val Resource = mockDB()

  protected val (sink, buffer) = sinkWithBuffer[T]

  override def before = Resource
}

class IntegrationMongoCleanUpSpec extends Specification {
  import MongoIntegrationEnv._

  private val logger = Logger.getLogger(classOf[IntegrationMongoCleanUpSpec])

  private def producers(id: Int) =
    query { b ⇒
      b.q("producer_num" $eq id)
      b.collection(PRODUCER)
      b.db(DB_NAME)
    } |> nameTransducer

  private def categories(e: (String, Buffer[Int])) = {
    query { b ⇒
      b.q("category" $in e._2)
      b.sort("name" $eq -1)
      b.collection(CATEGORY)
      b.db(DB_NAME)
    } map { obj ⇒ s"${e._1} - ${obj.get("name").asInstanceOf[String]}" }
  }

  private def categories0(ids: Buffer[Int]) = {
    query { b ⇒
      b.q("category" $in ids)
      b.sort("name" $eq -1)
      b.collection(CATEGORY)
      b.db(DB_NAME)
    }
  }

  "Hit server with multi conditional query" in new Enviroment[Int] {
    val products = query { b ⇒
      b.q("article" $gt 2 $lt 40)
      b.collection(PRODUCT)
      b.db(DB_NAME)
    }

    val p = for {
      dbObject ← Resource through (products |> articleIds).channel
      _ ← dbObject to sink
    } yield ()

    p.run.run
    buffer must be equalTo ArrayBuffer(3, 35)
  }

  "Hit server with sorted query with folding results in single value" in new Enviroment[String] {
    val products = query { b ⇒
      b.q("article" $gt 2 $lt 40)
      b.sort("article" $eq -1)
      b.collection(PRODUCT)
      b.db(DB_NAME)
    }

    val p = for {
      dbObject ← Resource through (products |> articleIds0).channel
      _ ← dbObject.fold1 { _ + "-" + _ } to sink
    } yield ()

    p.run.run
    buffer(0) must be equalTo "35-3"
  }

  "Hit server with native json query" in new Enviroment[String] {
    val products = query { b ⇒
      import b._
      q("""{ "article" : 1 } """)
      collection(PRODUCT)
      b.db(DB_NAME)
    }

    val p = for {
      dbObject ← Resource through (products |> nameTransducer).channel
      _ ← dbObject to sink
    } yield ()

    p.run.run
    buffer(0) must be equalTo "Extra Large Wheel Barrow"
  }

  "Hit server with join ONE-TO-MANY" in new Enviroment[String] {
    val products = query { b ⇒
      b.q(""" { "article": 1 } """)
      b.collection(PRODUCT)
      b.db(DB_NAME)
    } |> numTransducer

    val p = for {
      dbObject ← Resource through (for {
        n ← products
        prod ← producers(n)
      } yield (prod)).channel
      _ ← dbObject to sink
    } yield ()

    p.run.run
    //1 -> "Puma", "Reebok"
    buffer must be equalTo ArrayBuffer("Puma", "Reebok")
  }

  "Hit server with join ONE-TO-MANY with fold in single value" in new Enviroment[String] {
    val prodsWithCategoryIds = query { b ⇒
      b.q(Obj("article" -> 1).toString)
      b.collection(PRODUCT)
      b.db(DB_NAME)
    } |> categoryIds

    val p = for {
      dbObject ← Resource through (
        for {
          n ← prodsWithCategoryIds
          prod ← categories(n)
        } yield (prod)).channel
      _ ← dbObject.fold1 { _ + "/" + _ } to sink
    } yield ()

    p.run.run
    buffer(0) must be equalTo
      "Extra Large Wheel Barrow - Rubberized Work Glove/Extra Large Wheel Barrow - Gardening Tools/Extra Large Wheel Barrow - Car Tools"
  }

  "Hit server with join ONE-TO-MANY with monoid" in new Enviroment[String] {
    import scalaz._
    import Scalaz._
    import mongo.$in
    implicit val M = scalaz.Monoid[String]

    val prodsWithCategoryIds = query { b ⇒
      b.q(Obj("article" -> Obj(($in(), List(1, 2)))).toString)
      b.collection(PRODUCT)
      b.db(DB_NAME)
    } |> categoryIds

    val p = for {
      dbObject ← Resource through
        (for {
          n ← prodsWithCategoryIds
          prod ← categories0(n._2)
        } yield (prod)).channel
      _ ← dbObject.foldMap(_.get("name").asInstanceOf[String] + ", ") to sink
    } yield ()

    p.run.run
    logger.debug(buffer(0))
    buffer(0) must be equalTo "Rubberized Work Glove, Gardening Tools, Car Tools, Gardening Tools, Car1 Tools, "
  }

  "Use pipe and separate transducer for parse result " in new Enviroment[Int] {
    val products = query { b ⇒
      b.q(""" { "article": { "$gt" : 0, "$lt" : 4 } }""")
      b.collection(PRODUCT)
      b.db(DB_NAME)
      b.limit(2)
      b.skip(1)
    }

    val p = for {
      dbObject ← Resource through (products |> articleIds).channel
      _ ← dbObject to sink
    } yield ()

    p.runLog.run
    buffer must be equalTo ArrayBuffer(2, 3)
  }

  "Hit server with zip. Run 2 independent query sequentially and merge results in single value" in new Enviroment[String] {

    //if one process halts, so second process will halt too
    //even he didn't exhausted
    val resultTransducer = process1.lift({ pair: (DBObject, DBObject) ⇒
      pair._1.get("article").asInstanceOf[Int].toString + "-" +
        pair._2.get("category").asInstanceOf[Int].toString
    })

    val products = query { b ⇒
      b.q("article" $eq 1)
      b.collection(PRODUCT)
      b.db(DB_NAME)
    }

    val categories = query { b ⇒
      b.q("category" $eq 12)
      b.collection(CATEGORY)
      b.db(DB_NAME)
    }

    val p = for {
      dbObject ← Resource through ((products zip categories) |> resultTransducer).channel
      _ ← dbObject to sink
    } yield ()

    p.runLog.run
    buffer(0) must be equalTo "1-12"
  }

  "Hit server with zipWith" in new Enviroment[String] {
    //if one process halts, so second process will halt too
    implicit val dataExtracter = { (a: DBObject, b: DBObject) ⇒
      a.get("article").asInstanceOf[Int] + "-" +
        b.get("category").asInstanceOf[Int]
    }

    val products = query { b ⇒
      b.q("article" $in Seq(1, 2, 3))
      b.collection(PRODUCT)
      b.db(DB_NAME)
    }

    val categories = query { b ⇒
      b.q("category" $in Seq(12, 13))
      b.collection(CATEGORY)
      b.db(DB_NAME)
    }

    val tee = products zipWith categories

    val p = for {
      dbObject ← Resource through tee.channel
      _ ← dbObject to sink
    } yield ()

    p.runLog.run
    buffer must be equalTo ArrayBuffer("1-12", "2-13")
  }
}