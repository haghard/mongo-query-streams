package mongo.query

import mongo.streams.MongoQuery
import com.mongodb.MongoException

object Query {

  /**
   *
   * @param f
   * @return
   */
  def query(f: MutableQueryBuilder ⇒ Unit): MongoQuery = {
    val builder = new DefaultMutableQueryBuilder()
    f(builder)

    builder.build().fold(throw new MongoException("invalid query")) { dbObject ⇒
      MongoQuery(dbObject._1, dbObject._2, dbObject._3, dbObject._4, dbObject._5)
    }
  }
}