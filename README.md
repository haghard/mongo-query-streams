Mongo-query-streams
===================

Mongo query language

Design goals:  
  To provide mongo query creation in type safe manner (dsl, combinators api).
  Be able to use compositionality, expressiveness of scalaz-streams as advantage in mongo querying process.

There are several way to create mongo query in type safe manner and treat it like a scalaz-stream process

Using mongo.dsl._
```scala
    import mongo.query.Query.query
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
    import mongo.query.Query.query
    import mongo.dsl2._
    val q = Obj($and().op -> List(Obj("num" -> Obj(($gte(), 3), ($lt(), 10))), Obj("name" -> literal("Bauer"))))
    query { b ⇒
      b.q(q.toString)
      b.collection("tmp")
      b.db("test_db")
    }

```
Using native query

```scala
    import mongo.query.Query.query
    query { b ⇒
      b.q(""" { "article" : 1 } """)
      b.collection("tmp")
      b.db("test_db")
    }
    
```

Here's a basic example how to use processes:

```scala
  import scalaz.concurrent.Task
  import mongo.dsl._
  import scalaz.stream.process._
  import mongo.query.Query.default

  val client: MongoClient ...
  
  val P = eval(Task.delay(client))
  
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
    dbObject <- P through (products |> nameTransducer).channel
    _ <- dbObject to sink
  } yield ()
  
  p.onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); halt }
   .onComplete { eval(Task.delay(logger.debug(s"Interaction has been completed"))) }
   .runLog.run
   
  //result here
  buffer
   
```

Status
------
0.3 version