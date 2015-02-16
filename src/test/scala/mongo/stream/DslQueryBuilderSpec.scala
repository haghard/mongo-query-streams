package mongo.stream

import java.util.Date
import mongo.query.Query.default
import MongoIntegrationEnv.executor
import org.specs2.mutable.Specification
import org.specs2.specification.Snippets

class DslQueryBuilderSpec extends Specification with Snippets {

  override def is =
    s2"""

  ${"Build mongo query with QueryDsl".title}

  * Single selector query with eq operator ${
      snippet {
        import mongo.query.Query.query
        import mongo.dsl._
        query { b ⇒
          b.q("name" $eq "Taller")
          b.collection("tmp")
        }
      }
    }

  ${body.verifyEq}

  * Single selector query with gt operator ${
      snippet {
        import mongo.query.Query.query
        import mongo.dsl._
        query { b ⇒
          b.q("num" $gt 3)
          b.collection("tmp")
        }
      }
    }

  ${body.verifyGt}


  * Single selector query with gte operator ${
      snippet {
        import mongo.query.Query.query
        import mongo.dsl._
        query { b ⇒
          b.q("num" $gte 79.8)
          b.collection("tmp")
        }
      }
    }

  ${body.verifyGte}

 * Single selector query with lt operator ${
      snippet {
        import mongo.query.Query.query
        import mongo.dsl._
        query { b ⇒
          b.q("num" $lt 199.78)
          b.collection("tmp")
        }
      }
    }

 ${body.verifyLt}


 * Single selector query with lte operator ${
      snippet {
        import mongo.query.Query.query
        import mongo.dsl._
        query { b ⇒
          b.q("num" $lte 19.98)
          b.collection("tmp")
        }
      }
    }

 ${body.verifyLte}

 * Single selector query with ne operator ${
      snippet {
        import mongo.query.Query.query
        import mongo.dsl._
        query { b ⇒
          b.q("flag" $ne true)
          b.collection("tmp")
        }
      }
    }

 ${body.verifyNe}

 * Single selector query with in operator ${
      snippet {
        import mongo.query.Query.query
        import mongo.dsl._
        query { b ⇒
          b.q("num" $in Seq(1, 2, 4))
          b.collection("tmp")
        }
      }
    }

 ${body.verifyIn}

 * Single selector query with all operator ${
      snippet {
        import mongo.query.Query.query
        import mongo.dsl._
        query { b ⇒
          b.q("num" $all Seq(1, 2, 4))
          b.collection("tmp")
        }
      }
    }

 ${body.verifyAll}

 * Single selector query with nin operator ${
      snippet {
        import mongo.query.Query.query
        import mongo.dsl._
        query { b ⇒
          b.q("num" $nin Seq(1, 2, 4))
          b.collection("tmp")
        }
      }
    }

 ${body.verifyNin}


 * Single selector with nested conditions ${
      snippet {
        import mongo.query.Query.query
        import mongo.dsl._
        query { b ⇒
          b.q("num" $gt 3 $lt 20 $nin Seq(11, 12))
          b.collection("tmp")
        }
      }
    }

 ${body.verifyNested}

 * Logical AND query ${
      snippet {
        import mongo.query.Query.query
        import mongo.dsl._
        query { b ⇒
          b.q(&&("num" $gt 3, "name" $eq "James"))
          b.collection("tmp")
        }
      }
    }

 ${body.verifyAND}

 * Logical OR query ${
      snippet {
        import mongo.query.Query.query
        import mongo.dsl._
        query { b ⇒
          b.q(||("num" $lt 9.78, "num2" $gte 89.1))
          b.collection("tmp")
        }
      }
    }

 ${body.verifyOR}

 * Logical AND query with complex inner queries ${
      snippet {
        import mongo.query.Query.query
        import mongo.dsl._
        query { b ⇒
          b.q(||("num" $gte 3 $lt 10, "name" $eq "Jack Bauer"))
          b.collection("tmp")
        }
      }
    }

 ${body.verifyAND2}

 * Logical OR query with nested AND's ${
      snippet {
        import mongo.query.Query.query
        import mongo.dsl._
        query { b ⇒
          b.q(||(&&("num" $gte 178 $lte 199, "name" $eq "Jack Bauer"), &&("num" $gt 78 $lt 99, "name" $eq "James Bond")))
          b.collection("tmp")
        }
      }
    }
       
 ${body.verifyDt}

 ${body.verifyComplex}

 """
  def body = new {
    import mongo.query.Query.query
    import mongo.query.Query.default
    import MongoIntegrationEnv.executor
    import mongo.dsl._

    def verifyEq =
      query { b ⇒
        b.q("name" $eq "Taller")
        b.collection("tmp")
        b.db("db")
      } mustNotEqual null

    def verifyGt =
      query { b ⇒
        import b._
        q("num" $gt 3)
        collection("tmp")
      } mustNotEqual null

    def verifyGte = query { b ⇒
      import b._
      q("num" $gte 79.8)
      collection("tmp")
    } mustNotEqual null

    def verifyLt = query { b ⇒
      import b._
      q("num" $lt 199.78)
      collection("tmp")
    } mustNotEqual null

    def verifyLte = query { b ⇒
      import b._
      q(("num" $lt 19.98))
      collection("tmp")
    } mustNotEqual null

    def verifyNe = query { b ⇒
      import b._
      q(("flag" $ne true))
      collection("tmp")
    } mustNotEqual null

    def verifyIn = query { b ⇒
      b.q(("num" $in Seq(1, 2, 4)))
      b.collection("tmp")
    } mustNotEqual null

    def verifyAll = query { b ⇒
      b.q(("num" $all Seq(1, 2, 4)))
      b.collection("tmp")
    } mustNotEqual null

    def verifyNin = query { b ⇒
      b.q(("num" $nin Seq(1, 2, 4)))
      b.collection("tmp")
    } mustNotEqual null

    def verifyNested = query { b ⇒
      b.q(("num" $gt 3 $lt 20 $nin Seq(11, 12)))
      b.collection("tmp")
    } mustNotEqual null

    def verifyAND = query { b ⇒
      b.q(&&("num" $gt 13, "name" $eq "James"))
      b.collection("tmp")
    } mustNotEqual null

    def verifyOR = query { b ⇒
      b.q(||("num" $lt 9.78, "num2" $gte 89.1))
      b.collection("tmp")
    } mustNotEqual null

    def verifyAND2 = query { b ⇒
      b.q(&&("num" $gte 3 $lt 10, "name" $eq "Jack Bauer"))
      b.collection("tmp")
    } mustNotEqual null

    def verifyComplex = query { b ⇒
      b.q(||(&&("num" $gte 178 $lte 199, "name" $eq "Jack Bauer"), &&("num" $gt 78 $lt 99, "name" $eq "James Bond")))
      b.collection("tmp")
    } mustNotEqual null

    val dt = new Date()
    def verifyDt = query { b ⇒
      b.q("date" $gt dt)
      b.collection("tmp")
    } mustNotEqual null
  }
}