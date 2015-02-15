package mongo.query

import mongo.streams.{ MongoQuery, InvalidMongoQuery, DefaultMongoQuery }

object Query {

  /**
   *
   * @param f
   * @return
   */
  def query(f: MutableQueryBuilder ⇒ Unit): MongoQuery = {
    val builder = new DefaultMutableQueryBuilder()
    f(builder)

    builder.build().fold[MongoQuery](
      { e ⇒ new InvalidMongoQuery(e) },
      { s ⇒ new DefaultMongoQuery(s.q, s.collName, s.sortQuery, s.limit, s.skip, s.maxTimeMS) }
    )
  }
}