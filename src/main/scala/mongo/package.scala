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

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

package object mongo {

  //Supported values
  sealed trait Values[T]
  implicit val intV = new Values[Int] {}
  implicit val longV = new Values[Long] {}
  implicit val doubleV = new Values[Double] {}
  implicit val stringV = new Values[String] {}
  implicit val booleanV = new Values[Boolean] {}
  implicit val dateV = new Values[Date] {}

  trait MqlExpression

  def formatter() = new SimpleDateFormat("dd MMM yyyy hh:mm:ss:SSS a z")

  sealed trait MqlOp extends MqlExpression {
    def op: String
  }

  case class $gt(override val op: String = "$gt") extends MqlOp

  case class $gte(override val op: String = "$gte") extends MqlOp

  case class $lt(override val op: String = "$lt") extends MqlOp

  case class $lte(override val op: String = "$lte") extends MqlOp

  case class $eq(override val op: String = "$eq") extends MqlOp

  //set operators
  case class $in(override val op: String = "$in") extends MqlOp
  case class $all(override val op: String = "$all") extends MqlOp
  case class $nin(override val op: String = "$nin") extends MqlOp

  //boolean operators
  case class $and(override val op: String = "$and") extends MqlOp
  case class $or(override val op: String = "$or") extends MqlOp
  case class $ne(override val op: String = "$ne") extends MqlOp

  /**
   *
   * @param name
   */
  final class NamedThreadFactory(val name: String) extends ThreadFactory {
    private def namePrefix = name + "-thread-"
    private val threadNumber = new AtomicInteger(1)
    private val group: ThreadGroup = Thread.currentThread().getThreadGroup

    def newThread(r: Runnable) =
      new Thread(this.group, r, namePrefix + this.threadNumber.getAndIncrement(), 0L)
  }
}
