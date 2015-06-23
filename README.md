Mongo-query-streams
===================

Mongo query language

Design goals:  
  * Provide mongo query creation in type safe manner
  * Write resource safe code
  * Use compositionality, expressiveness of scalaz-streams as advantage in mongo querying
  * Consider the result as [ScalazStream](https://github.com/scalaz/scalaz-stream) or [RxScala](https://github.com/ReactiveX/RxScala.git).

Getting Started
===================
First, you will need to add the Bintray resolver settings to your SBT file:
```scala
    resolvers += "bintray-repo" at "http://dl.bintray.com/haghard/releases"    
```
and
 ```scala
   libraryDependencies += "org.mongo.scalaz"    %% "mongo-query-streams" %  "0.6.5"   
 ```

Examples
===================
There are several way to create mongo query in type safe manner and treat it like a scalaz-stream process

Using native query which will be parser and validated

```scala
    import mongo.query._
    create { b ⇒
      b.q(""" { "article" : 1 } """)
      b.collection("tmp")
      b.db("test_db")
    }
    
```

Using mongo.dsl._
```scala
    import mongo._
    import query._    
    create { b ⇒
      b.q(&&("num" $gt 3, "name" $eq "James"))
      b.sort("num" $eq -1)
      b.collection("tmp")
      b.db("test_db")
    }
    
```

Using mongo.dsl2_
```scala
    import mongo._
    import query._
    import dsl2._
    val q = Obj($and().op -> List(Obj("num" -> Obj(($gte(), 3), ($lt(), 10))), Obj("name" -> literal("Bauer"))))
    create { b ⇒
      b.q(q.toString)
      b.collection("tmp")
      b.db("test_db")
    }
    
```

Using monadic query composition
```scala
    import mongo._    
    import dsl._    
    
    val query = for {
    _ ← "producer_num" $eq 1
    x ← "article" $gt 0 $lt 6 $nin Seq(4, 5)
    } yield x
    
    //string 
    query.toQuery    
    //DBObject
    query.toDBObject    
```

Using package dsl3 you can easy fetch one/batch/stream  

```scala
    import mongo._
    import dsl3._
    import Query._
    import Interaction._
    import rx.lang.scala.{ Observable, Subscriber }
  
    val query = for {
      _ ← "producer_num" $eq 1
      q ← "article" $gt 0 $lt 6 $nin Seq(4, 5)
    } yield q
    
    //scalar result
    query.findOne(client, DB_NAME, PRODUCT).attemptRun
    
    //batch result
    query.list(client, DB_NAME, PRODUCT).attemptRun    
    
    //or stream of BasicDBObject     
    query.stream[MProcess](TEST_DB, LANGS)
    
    //or stream of Strings from field "f" using Process
    query.streamC[MStream](TEST_DB, LANGS).column[String]("f")    
    
    //or stream of Int from field "f2" using Observable    
    query.streamC[Observable](TEST_DB, LANGS).column[Int]("f2")
    
```  

Here's a basic example how to build query, run and get results:

```scala
  import mongo_  
  import query._
  import dsl._
  import scalaz.concurrent.Task
  import scalaz.stream.process._
  import scalaz.stream.Process

  val client: MongoClient = ...
  val Resource = eval(Task.delay(client))
  
  val buffer: Buffer[Int] = Buffer.empty
  val sink = scalaz.stream.io.fillBuffer(buffer)
  
  implicit val exec = newFixedThreadPool(2, new NamedThreadFactory("db-worker"))

  val products = create { b ⇒
    b.q("article" $gt 2 $lt 40)
    b.collection(PRODUCT)
    b.db(TEST_DB)
  }.column[Int]("article")
  
   (for {
    article ← Resource through products.out
    _ ← article to sink
   } yield ())          
    .onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); halt }
    .onComplete(P.eval(Task.delay(logger.debug(s"Interaction has been completed"))))
    .run.run
   
  //result here
  buffer
   
```

Big win there is that `products` value incapsulates a full interaction lifecycle for with mongo client (get db by name, get collection by name, submit query with preferences, fetch records from cursor, close the cursor). If exception occurs cursor will be closed.

We do support join between 2 collections and 2 different streaming library [RxScala](https://github.com/ReactiveX/RxScala.git) and [ScalazStream](https://github.com/scalaz/scalaz-stream) through single type `mongo.join.Join` which can by parametrized with `ProcessStream` and `ObservableStream`   

We have two methods for join collections: `joinByPk` and `join`. If you fine with output type from left stream only with key field you should use `joinByPk`. If you aren't, than use `join` for unlimited possibilities in output type.
     
Here's a example of how you can do joinByPk between collections `LANGS` and `PROGRAMMERS` by `LANGS.index == PROGRAMMERS.lang` using `Scalaz Streams`

```scala
  import mongo._
  import join._    
  import dsl3._
  import Query._
  import scalaz.stream.Process
  import mongo.join.process.ProcessStream
  
  val buffer = Buffer.empty[String]
  val Sink = scalaz.stream.io.fillBuffer(buffer)
    
  val qLang = for { q ← "index" $gte 0 $lte 5 } yield q
  def qProg(id: Int) = for { q ← "lang" $eq id } yield q
  
  implicit val exec = newFixedThreadPool(2, new NamedThreadFactory("db-worker"))
  implicit val c = client
  val joiner = Join[ProcessS]
      
  val query = joiner.joinByPk(qLang, LANGS, "index", qProg(_: Int), PROGRAMMERS, TEST_DB) { (l, r: DBObject) ⇒
    s"Primary-key:$l - val:[Foreign-key:${r.get("lang")} - ${r.get("name")}]"
  }              
          
  for {
    e ← Process.eval(Task.delay(client)) through query.out
    _ ← e to Sink
  } yield ()
    .onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); halt }
    .onComplete(P.eval(Task.delay(logger.debug(s"Interaction has been completed"))))
    .run.run      
  
```

Join using `rx.lang.scala.Observable`

```scala

  import mongo._
  import join._
  import dsl3._
  import Query._
  import rx.lang.scala.Subscriber  
  import rx.lang.scala.schedulers.ExecutionContextScheduler
  import mongo.join.observable.ObservableStream
  
  val buffer = Buffer.empty[String]
  val Sink = io.fillBuffer(buffer)
      
  val qLang = for { q ← "index" $gte 0 $lte 5 } yield q
  def qProg(id: Int) = for { q ← "lang" $eq id } yield q
  
  implicit val exec = newFixedThreadPool(2, new NamedThreadFactory("db-worker"))
  implicit val c = client
  val joiner = Join[ObservableS]
  
  val query = joiner.joinByPk(qLang, LANGS, "index", qProg(_: Int), PROGRAMMERS, TEST_DB) { (l, r: DBObject) ⇒
    s"Primary-key:$l - val:[Foreign-key:${r.get("lang")} - ${r.get("name")}]"
  }
  
  val testSubs = new Subscriber[String] {
    override def onStart(): Unit = request(1)
    override def onNext(n: String) = request(1)    
    override def onError(e: Throwable) = logger.info(s"OnError: ${e.getMessage}")
    override def onCompleted(): Unit = logger.info("Interaction has been completed")          
  }
  
  query.observeOn(ExecutionContextScheduler(ExecutionContext.fromExecutor(executor)))
    .subscribe(testSubs)
    
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
0.6.5 version