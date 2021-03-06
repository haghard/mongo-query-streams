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

import java.util.Arrays._
import java.util.Date
import MongoIntegrationEnv.executor
import com.mongodb.BasicDBObject
import org.specs2.mutable.Specification
import org.specs2.specification.Snippets

class DslQueryBuilderSpec extends Specification with Snippets {
  override def is = s2"""

   Build mongo query with QueryDsl
   ===============================

  * Single selector query with eq operator ${
    snippet {
      import mongo._
      import query._
      create { b ⇒
        b.q("name" $eq "Taller")
        b.collection("tmp")
      }
    }
  }

  ${body.verifyEq}

  * Single selector query with gt operator ${
    snippet {
      import mongo._
      import query._
      create { b ⇒
        b.q("num" $gt 3)
        b.collection("tmp")
      }
    }
  }

  ${body.verifyGt}


  * Single selector query with gte operator ${
    snippet {
      import mongo._
      import query._
      create { b ⇒
        b.q("num" $gte 79.8)
        b.collection("tmp")
      }
    }
  }

  ${body.verifyGte}

 * Single selector query with lt operator ${
    snippet {
      import mongo._
      import query._
      create { b ⇒
        b.q("num" $lt 199.78)
        b.collection("tmp")
      }
    }
  }

 ${body.verifyLt}


 * Single selector query with lte operator ${
    snippet {
      import mongo._
      import query._
      create { b ⇒
        b.q("num" $lte 19.98)
        b.collection("tmp")
      }
    }
  }

 ${body.verifyLte}

 * Single selector query with ne operator ${
    snippet {
      import mongo._
      import query._
      create { b ⇒
        b.q("flag" $ne true)
        b.collection("tmp")
      }
    }
  }

 ${body.verifyNe}

 * Single selector query with in operator ${
    snippet {
      import mongo._
      import query._
      create { b ⇒
        b.q("num" $in Seq(1, 2, 4))
        b.collection("tmp")
      }
    }
  }

 ${body.verifyIn}

 * Single selector query with all operator ${
    snippet {
      import mongo._
      import query._
      create { b ⇒
        b.q("num" $all Seq(1, 2, 4))
        b.collection("tmp")
      }
    }
  }

 ${body.verifyAll}

 * Single selector query with nin operator ${
    snippet {
      import mongo._
      import query._
      create { b ⇒
        b.q("num" $nin Seq(1, 2, 4))
        b.collection("tmp")
      }
    }
  }

 ${body.verifyNin}


 * Single selector with nested conditions ${
    snippet {
      import mongo._
      import query._
      create { b ⇒
        b.q("num" $gt 3 $lt 20 $nin Seq(11, 12))
        b.collection("tmp")
      }
    }
  }

 ${body.verifyNested}

 * Logical AND query ${
    snippet {
      import mongo._
      import query._
      create { b ⇒
        b.q(&&("num" $gt 3, "name" $eq "James"))
        b.collection("tmp")
      }
    }
  }

 ${body.verifyAND}

 * Logical OR query ${
    snippet {
      import mongo._
      import query._
      create { b ⇒
        b.q(||("num" $lt 9.78, "num2" $gte 89.1))
        b.collection("tmp")
      }
    }
  }

 ${body.verifyOR}

 * Logical AND query with complex inner queries ${
    snippet {
      import mongo._
      import query._
      create { b ⇒
        b.q(||("num" $gte 3 $lt 10, "name" $eq "Jack Bauer"))
        b.collection("tmp")
      }
    }
  }

 ${body.verifyAND2}

 * Logical OR query with nested AND's ${
    snippet {
      import mongo._
      import query._
      create { b ⇒
        b.q(||(&&("num" $gte 178 $lte 199, "name" $eq "Jack Bauer"), &&("num" $gt 78 $lt 99, "name" $eq "James Bond")))
        b.collection("tmp")
      }
    }
  }
       
 ${body.verifyDt}
 * Single selector query with date argument ${
    snippet {
      import mongo._
      import query._
      create { b ⇒
        b.q("date" $gt new Date())
        b.collection("tmp")
      }
    }
  }

 ${body.verifyOrAnd}
  * Single selector query with Or and nested AND's ${
    snippet {
      import mongo._
      import query._
      create { b ⇒
        b.q(||(&&("num" $gte 178 $lte 199, "name" $eq "Jack Bauer"), &&("num" $gt 78 $lt 99, "name" $eq "James Bond")))
        b.collection("tmp")
      }
    }
  }

  ${body.verifyMonadicQuery}
  * Monadic query ${
    snippet {
      import mongo._
      import dsl._
      import qb._

      for {
        _ ← "producer_num" $eq 1
        _ ← "article" $gt 0 $lt 6 $nin Seq(4, 5)
        q ← sort("producer_num" -> Order.Ascending)
      } yield q
    }
  }

  """
  def body = new {

    import mongo._
    import query._
    import dsl._
    import qb._
    import MongoIntegrationEnv.executor

    def verifyEq =
      create { b ⇒
        b.q("name" $eq "Taller")
        b.collection("tmp")
        b.db("db")
      } mustNotEqual null

    def verifyGt =
      create { b ⇒
        import b._
        q("num" $gt 3)
        collection("tmp")
      } mustNotEqual null

    def verifyGte = create { b ⇒
      import b._
      q("num" $gte 79.8)
      collection("tmp")
    } mustNotEqual null

    def verifyLt = create { b ⇒
      import b._
      q("num" $lt 199.78)
      collection("tmp")
    } mustNotEqual null

    def verifyLte = create { b ⇒
      import b._
      q(("num" $lt 19.98))
      collection("tmp")
    } mustNotEqual null

    def verifyNe = create { b ⇒
      import b._
      q(("flag" $ne true))
      collection("tmp")
    } mustNotEqual null

    def verifyIn = create { b ⇒
      b.q(("num" $in Seq(1, 2, 4)))
      b.collection("tmp")
    } mustNotEqual null

    def verifyAll = create { b ⇒
      b.q(("num" $all Seq(1, 2, 4)))
      b.collection("tmp")
    } mustNotEqual null

    def verifyNin = create { b ⇒
      b.q(("num" $nin Seq(1, 2, 4)))
      b.collection("tmp")
    } mustNotEqual null

    def verifyNested = create { b ⇒
      b.q(("num" $gt 3 $lt 20 $nin Seq(11, 12)))
      b.collection("tmp")
    } mustNotEqual null

    def verifyAND = create { b ⇒
      b.q(&&("num" $gt 13, "name" $eq "James"))
      b.collection("tmp")
    } mustNotEqual null

    def verifyOR = create { b ⇒
      b.q(||("num" $lt 9.78, "num2" $gte 89.1))
      b.collection("tmp")
    } mustNotEqual null

    def verifyAND2 = create { b ⇒
      b.q(&&("num" $gte 3 $lt 10, "name" $eq "Jack Bauer"))
      b.collection("tmp")
    } mustNotEqual null

    def verifyOrAnd = create { b ⇒
      b.q(||(&&("num" $gte 178 $lte 199, "name" $eq "Jack Bauer"), &&("num" $gt 78 $lt 99, "name" $eq "James Bond")))
      b.collection("tmp")
    } mustNotEqual null

    def verifyDt = create { b ⇒
      b.q("date" $gt new Date())
      b.collection("tmp")
    } mustNotEqual null

    def verifyMonadicQuery = {
      val expected = new BasicDBObject("article",
        new BasicDBObject("$gt", 0).append("$lt", 6).append("$nin", asList(4, 5))).append("producer_num", 1)
      (for {
        _ ← "producer_num" $eq 1
        _ ← "article" $gt 0 $lt 6 $nin Seq(4, 5)
        q ← sort("producer_num" -> Order.Ascending)
      } yield q).toDBObject mustEqual expected
    }
  }
}
