Mongo-query-streams
============

Mongo query language

Design goals:  
  To provide mongo query creation in type safe manner (dsl, combinators api).
  Be able to use compositionality, expressiveness of scalaz-streams as advantage in mongo querying process.

There are several way to create mongo query in type safe manner and treat it like a scalaz-stream process

Using mongo.dsl.QueryDsl
```scala
    import mongo.query.Query.query
    import mongo.dsl.QueryDsl._
    query { b ⇒
      b.q(&&("num" $gt 3, "name" $eq "James"))
      b.sort("num" $eq -1)
      b.collection("tmp")
    }.toProcess
    
```

Using mongo.dsl.CombinatorDsl
```scala
    import mongo._
    import mongo.query.Query.query
    import mongo.dsl.CombinatorDsl._
    val q = Obj($and().op -> List(Obj("num" -> Obj(($gte(), 3), ($lt(), 10))), Obj("name" -> literal("Bauer"))))
    query { b ⇒
      b.q(q)
      b.collection("tmp")
    }.toProcess

```
Using native

```scala
    import mongo.query.Query.query
    query { b ⇒
      b.q(""" { "article" : 1 } """)
      b.collection("tmp")
    }.toProcess
    
```

Here's a basic example how to use processes:

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