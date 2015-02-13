Mongo-query-streams
============

Mongo query language

Design goals:  
  To provide mongo query creation in type safe manner (dsl, combinators api).
  Be able to use compositionality, expressiveness of scalaz-streams as advantage in mongo querying process.

Here's a basic example how to use it:

```scala
  import mongo.dsl.QueryDsl._

  val Resource = mockDB()
      
  implicit val mongoExecutor =
      Executors.newFixedThreadPool(5, new NamedThreadFactory("mongo-worker"))
  
  val buffer: Buffer[String] = Buffer.empty
  val sink = scalaz.stream.io.fillBuffer(buffer)
  
  val nameTransducer = process1.lift({ obj: DBObject ⇒ obj.get("name").toString })

  val products = query { b ⇒
    b.q("article" $gt 2 $lt 40)
    b.collection(PRODUCT)
  }.toProcess

  val p = for {
    dbObject <- Resource through (products |> nameTransducer).channel
    _ <- dbObject to sink
  } yield ()
    
  p.runLog.run
```

Status
------
0.1 version