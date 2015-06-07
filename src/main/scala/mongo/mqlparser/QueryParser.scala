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

package mongo.mqlparser

import scalaz._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.traverse._

object QueryParser {
  val P = new scalaparsers.Parsing[Unit] {}
  import P._
  import scalaparsers.Diagnostic._

  case class Pair(name: String, v: MqlValue)

  implicit class ParserOps[A](p: Parser[A]) {
    def parse(s: String) = runParser(p, s) match {
      case Left(e)       ⇒ \/.left(e.pretty.toString)
      case Right((_, r)) ⇒ \/.right(r)
    }
    def |:(s: String) = p scope s
  }

  lazy val kvs: Parser[List[Pair]] = "kvs" |: {
    directives << skipLWS << realEOF
  }

  lazy val directives: Parser[List[Pair]] =
    binding.map2(attempt(newline >> binding).many)(_ :: _)

  lazy val newline: Parser[Unit] =
    "newline" |: satisfy(c ⇒ c == '\r' || c == '\n').skip

  lazy val whitespace: Parser[Unit] =
    "whitespace" |: satisfy(c ⇒ c.isWhitespace && c != '\r' && c != '\n').skip

  lazy val comment: Parser[Unit] = "comment" |: {
    ch('#').attempt >>
      satisfy(c ⇒ c != '\r' && c != '\n').skipMany >>
      (newline | realEOF) >>
      unit(())
  }

  private lazy val stringLetter = satisfy(c ⇒ (c != '"') && (c != '\\') && (c > 22.toChar))

  private val charEscMagic: Map[Char, Char] = "bfnrt\\\"'".zip("\b\f\n\r\t\\\"'").toMap

  private lazy val escapeCode = "escape code" |:
    choice(charEscMagic.toSeq.map { case (c, d) ⇒ ch(c) as d }: _*)

  private lazy val stringEscape = ch('\\') >> {
    (satisfy(_.isWhitespace).skipSome >> ch('\\')).as(None) |
      escapeCode.map(Some(_))
  }

  lazy val stringChar = stringLetter.map(Some(_)) | stringEscape

  lazy val string: Parser[String] = "string literal" |:
    stringChar.many.between('"', '"').map(_.sequence[Option, Char].getOrElse(List()).mkString)

  def takeWhile(p: Char ⇒ Boolean): Parser[List[Char]] = satisfy(p).many
  def isCont(c: Char) = Character.isLetterOrDigit(c) || c == '_' || c == '-'

  lazy val ident: Parser[String] = for {
    n ← satisfy(c ⇒ Character.isLetter(c)).map2(takeWhile(isCont))(_ +: _)
    _ ← failWhen[Parser](n == "import", s"reserved word ($n) used as an identifier")
  } yield n.mkString

  lazy val binding = "binding" |: {
    attempt(ident << skipLWS << '=' << skipLWS).map2(value) { (x, v) ⇒ Pair(x, v) }
  }

  lazy val signedInt: Parser[BigInt] =
    (ch('-') >> decimal).map(-_) | (ch('+') >> decimal) | decimal

  lazy val scientific: Parser[BigDecimal] = "numeric literal" |: {
    for {
      positive ← satisfy(c ⇒ c == '-' || c == '+').map(_ == '+') | unit(true)
      n ← decimal
      s ← (satisfy(_ == '.') >> takeWhile(_.isDigit).map(f ⇒
        BigDecimal(n + "." + f.mkString))) | unit(BigDecimal(n))
      sCoeff = if (positive) s else (-s)
      r ← satisfy(c ⇒ c == 'e' || c == 'E') >>
        signedInt.flatMap(x ⇒
          if (x > Int.MaxValue) fail[Parser](s"Exponent too large: $x")
          else unit(s * BigDecimal(10).pow(x.toInt))) | unit(sCoeff)
    } yield r
  }

  lazy val value: Parser[MqlValue] = "value literal" |: choice(
    string.map(MqlString(_)),
    scientific.map(d ⇒ MqlDouble(d.toDouble))
  )

  // Skip lines, comments, or horizontal white space
  lazy val skipLWS: Parser[Unit] = (newline | comment | whitespace).skipMany

  lazy val digit = "digit" |: satisfy(_.isDigit)

  lazy val decimal: Parser[BigInt] =
    digit.some.map(_.foldLeft(BigInt(0))(addDigit))

  private def addDigit(a: BigInt, c: Char) = a * 10 + (c - 48)

  import scalaparsers.ParseState
  import scalaparsers.Supply
  import scalaparsers.Pos

  def runParser[A](p: Parser[A], input: String, fileName: String = "") =
    p.run(ParseState(
      loc = Pos.start(fileName, input),
      input = input,
      s = (),
      layoutStack = List()), Supply.create)
}
