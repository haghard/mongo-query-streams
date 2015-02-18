Mongo-query-streams
===================

Mongo query language

Design goals:  
  * To provide mongo query creation in type safe manner
  * Write resource safe code
  * Be able to use compositionality, expressiveness of scalaz-streams as advantage in mongo querying

There are several way to create mongo query in type safe manner and treat it like a scalaz-stream process

Using mongo.dsl._
```scala
    import mongo.query._
    import mongo.dsl._
    query { b ⇒
      b.q(&&("num" $gt 3, "name" $eq "James"))
      b.sort("num" $eq -1)
      b.collection("tmp")
      b.db("test_db")
    }
    
```

Using mongo.dsl2_
```scala
    import mongo._
    import mongo.query._
    import mongo.dsl2._
    val q = Obj($and().op -> List(Obj("num" -> Obj(($gte(), 3), ($lt(), 10))), Obj("name" -> literal("Bauer"))))
    query { b ⇒
      b.q(q.toString)
      b.collection("tmp")
      b.db("test_db")
    }
    
```

Using mongo.dsl and free
```scala
    import mongo.dsl._
    import free._

    val program = for {
      _ ← "article" $gt 0 $lt 4
      x ← "producer_num" $gt 0
    } yield x
    
    val query = instructions(program)
```

Using native query

```scala
    import mongo.query._
    query { b ⇒
      b.q(""" { "article" : 1 } """)
      b.collection("tmp")
      b.db("test_db")
    }
    
```

Here's a basic example how to use processes for simple query:

```scala
  import mongo.dsl._
  import mongo.query._
  import scalaz.concurrent.Task
  import scalaz.stream.process._

  val client: MongoClient ...
  val Resource = eval(Task.delay(client))
  
  val buffer: Buffer[String] = Buffer.empty
  val sink = scalaz.stream.io.fillBuffer(buffer)
  val nameTransducer = process1.lift({ obj: DBObject ⇒ obj.get("name").toString })
  
  implicit val mongoExecutor = 
    Executors.newFixedThreadPool(5, new NamedThreadFactory("mongo-worker"))

  val products = query { b ⇒
    b.q("article" $gt 2 $lt 40)
    b.collection(PRODUCT)
    b.db("test_db")
  }

  val p = for {
    dbObject <- Resource through (products |> nameTransducer).channel
    _ <- observe EnvLogger to sink
  } yield ()
  
  p.onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); halt }
   .onComplete { eval(Task.delay(logger.debug(s"Interaction has been completed"))) }
   .runLog.run
   
  //result here
  buffer
   
```

Big win there is that `products` value incapsulates a full lifecycle of working
with mongo client (get db by name, get collection by name, submit query with preferences, 
fetch records from cursor, close the cursor when he is exhausted). Cursor will be closed even
in exception case.


Here's a example of how you can do join with to collections:

```scala
  import mongo.dsl._
  import mongo.query._
  import scalaz.concurrent.Task
  import scalaz.stream.process._
  import scalaz._
  import Scalaz._
  implicit val M = scalaz.Monoid[String]
  
  def EnvLogger(): scalaz.stream.Sink[Task, String] = ...
   
  val client: MongoClient ...
  val Resource = eval(Task.delay(client))

  def categories(e: (String, Buffer[Int])) = {
    query { b ⇒
      b.q("category" $in e._2)
      b.sort("name" $eq -1)
      b.collection(CATEGORY)
      b.db(DB_NAME)
    }
  }
    
  val prodsWithCatIds = query { b ⇒
    b.q(Obj("article" -> 1).toString)
    b.collection(PRODUCT)
    b.db(DB_NAME)
  }
    
  val p = for {
    dbObject ← Resource through
      (for {
        n ← prodsWithCatIds
        prod ← categories(n)
      } yield (prod)).channel
    _ ← dbObject.foldMap(_.get("name").asInstanceOf[String] + ", ") observe EnvLogger to sink
  } yield ()
    
  p.onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); halt }
    .onComplete { eval(Task.delay(logger.debug(s"Interaction has been completed"))) }
    .runLog.run

  //result here
  buffer
```

To run tests:
  <code>sbt test</code>

  html pages:
<code>sbt test-only -- html</code>

  markdown files:
<code>sbt test-only -- markdown</code>


To run output on console
  <code>test-only -- console</code>
  
Generated files can be found in /target/spec2-reports

Status
------
0.5 version