package mongo

import java.util.Date

package object dsl2 {

  type Effect = StringBuilder ⇒ Unit
  type Par = (String, Action)
  type KVS = (String, TraversableOnce[Par])

  object Action {
    import scala.xml.Utility.{ escape ⇒ esc }

    private[dsl2] def s(s: String): Action = Action(sb ⇒ sb append s)

    private[dsl2] def value[T](s: T): Action = Action(sb ⇒ sb append s)

    private[dsl2] def escape(s: String): Action = Action(sb ⇒ sb append esc(s))

    private[dsl2] def intersperse(delim: Action)(as: TraversableOnce[Action]) = Action { sb ⇒
      var between = false
      as foreach { a ⇒
        if (between) {
          delim(sb)
          a(sb)
        } else {
          a(sb)
          between = true
        }
      }
    }

    private[dsl2] def entry(key: String, value: Action): Action =
      literal(key) ++ s(" : ") ++ value

    private[dsl2] def obj(entries: TraversableOnce[Par]): Action =
      s("{ ") ++ intersperse(s(", "))(entries.map(p ⇒ entry(p._1, p._2))) ++ s(" }")

    private[dsl2] def list(values: TraversableOnce[Action]): Action =
      s("[") ++ intersperse(s(", "))(values) ++ s("]")

    private[dsl2] def nestedMap(entries: TraversableOnce[KVS]): Action = Action { sb ⇒
      var start = true
      entries foreach { en ⇒
        if (start) { start = false; s("{ ")(sb) }
        else s(" , ")(sb)
        (literal(en._1) ++ s(" : ") ++ obj(en._2))(sb)
      }
      if (!start) s(" } ")(sb)
    }
  }

  final case class Action(f: Effect) extends Effect {
    def apply(sb: StringBuilder) = f(sb)

    def ++(other: Action) = Action { sb ⇒
      apply(sb)
      other(sb)
    }

    override def toString: String = {
      val sb = new StringBuilder
      apply(sb)
      sb.toString
    }
  }

  import Action._

  def literal(l: String): Action = s("\"") ++ escape(l) ++ s("\"")

  def NestedMap(entries: KVS*): Action = nestedMap(entries)

  def List(values: Action*): Action = list(values)

  def Obj(entries: Par*): Action = obj(entries)

  implicit def str2LiteralAction(s: String) = literal(s)

  implicit def value2Action[T: Values](n: T): Action = {
    val item = n match {
      case date: Date ⇒ literal(formatter.format(date))
      case other      ⇒ n
    }
    value(item)
  }

  implicit def op2Name[T <: MqlOp](operation: T): String = operation.op
}
