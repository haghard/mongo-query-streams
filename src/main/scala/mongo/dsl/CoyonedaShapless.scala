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

package mongo.dsl

import shapeless.ops.coproduct.Inject
import shapeless.{ :+:, CNil, Coproduct, Inl, Inr }

import scalaz.Coyoneda.CoyonedaF
import scalaz.{ Coyoneda, Free, Functor, Monad, ~> }

object CoyonedaShapless {

  def liftCoyo[F[_], G[_]](fg: F ~> G): CoyonedaF[F]#A ~> CoyonedaF[G]#A =
    new (Coyoneda.CoyonedaF[F]#A ~> Coyoneda.CoyonedaF[G]#A) {
      def apply[A](c: Coyoneda[F, A]) = {
        Coyoneda.apply(fg(c.fi))(c.k)
      }
    }

  def liftCoyoLeft[F[_], G[_]: Functor](fg: F ~> G): CoyonedaF[F]#A ~> G = {
    type CF[A] = Coyoneda[F, A]
    type CG[A] = Coyoneda[G, A]

    val m: (CF ~> CG) = liftCoyo(fg)

    new (CF ~> G) {
      def apply[A](c: CF[A]) = m(c).run
    }
  }

  // Lifts a F ~> G into Free[F, _] ~> G running the free and using original NatTrans
  def liftFree[F[_]: Functor, G[_]: Monad](fg: F ~> G): ({ type l[A] = Free[F, A] })#l ~> G = {
    new (({ type l[A] = Free[F, A] })#l ~> G) {
      def apply[A](free: Free[F, A]) = free.foldMap(fg)
    }
  }

  /** Helper to inject a F[A] into Coproduct into Coyoneda into FreeMonad */
  class Copoyo[C[_] <: Coproduct] {
    def apply[F[_], A](fa: F[A])(implicit inj: Inject[C[A], F[A]]): Free.FreeC[C, A] =
      Free.liftFC(Coproduct[C[A]](fa))
  }

  object Copoyo {
    def apply[C[_] <: Coproduct] = new Copoyo[C]
  }

  implicit class RichNatT[F[_], R[_]](val f: F ~> R) extends AnyVal {
    def ||:[G[_]](g: G ~> R) = {
      new ~>[({ type l[T] = G[T]:+: F[T]:+: CNil })#l, R] {
        def apply[T](c: G[T] :+: F[T] :+: CNil) = c match {
          case Inl(h)      ⇒ g(h)
          case Inr(Inl(t)) ⇒ f(t)
          case _           ⇒ throw new RuntimeException("impossible case")
        }
      }
    }
  }

  implicit class RichNatT2[G[_], H[_], R[_]](val g: ({ type l[T] = (G[T] :+: H[T] :+: CNil) })#l ~> R) {
    def ||:[F[_]](f: F ~> R) = {
      new ~>[({ type l[T] = F[T]:+: G[T]:+: H[T]:+: CNil })#l, R] {
        def apply[T](c: F[T] :+: G[T] :+: H[T] :+: CNil) = c match {
          case Inl(h) ⇒ f(h)
          case Inr(t) ⇒ g(t)
          case _      ⇒ throw new RuntimeException("impossible case")
        }
      }
    }
  }

  implicit class RichNatT3[G[_], H[_], I[_], R[_]](val g: ({ type l[T] = (G[T] :+: H[T] :+: I[T] :+: CNil) })#l ~> R) {
    def ||:[F[_]](f: F ~> R) = {
      new ~>[({ type l[T] = F[T]:+: G[T]:+: H[T]:+: I[T]:+: CNil })#l, R] {
        def apply[T](c: F[T] :+: G[T] :+: H[T] :+: I[T] :+: CNil) = c match {
          case Inl(h) ⇒ f(h)
          case Inr(t) ⇒ g(t)
          case _      ⇒ throw new RuntimeException("impossible case")
        }
      }
    }
  }

  implicit class RichNatT4[G[_], H[_], I[_], J[_], R[_]](val g: ({ type l[T] = (G[T] :+: H[T] :+: I[T] :+: J[T] :+: CNil) })#l ~> R) {
    def ||:[F[_]](f: F ~> R) = {
      new ~>[({ type l[T] = F[T]:+: G[T]:+: H[T]:+: I[T]:+: J[T]:+: CNil })#l, R] {
        def apply[T](c: F[T] :+: G[T] :+: H[T] :+: I[T] :+: J[T] :+: CNil) = c match {
          case Inl(h) ⇒ f(h)
          case Inr(t) ⇒ g(t)
          case _      ⇒ throw new RuntimeException("impossible case")
        }
      }
    }
  }

  /** Coproduct Functors */
  implicit def CoproductFunctor1[F[_]](implicit F: Functor[F]) =
    new Functor[({ type l[A] = F[A]:+: CNil })#l] {
      def map[A, B](fa: F[A] :+: CNil)(f: A ⇒ B): F[B] :+: CNil = fa match {
        case Inl(h) ⇒ Coproduct[F[B] :+: CNil](F.map(h)(f))
        case Inr(t) ⇒ throw new RuntimeException("impossible case")
        case _      ⇒ throw new RuntimeException("impossible case")
      }

    }

  implicit def CoproductFunctor2[F[_], G[_]](implicit F: Functor[F], G: Functor[({ type l[A] = G[A] :+: CNil })#l]) =
    new Functor[({ type l[A] = F[A]:+: G[A]:+: CNil })#l] {
      import Coproduct._
      def map[A, B](fa: F[A] :+: G[A] :+: CNil)(f: A ⇒ B): F[B] :+: G[B] :+: CNil = fa match {
        case Inl(h) ⇒ Coproduct[F[B] :+: G[B] :+: CNil](F.map(h)(f))
        case Inr(t) ⇒ G.map(t)(f).extendLeft[F[B]]
        case _      ⇒ throw new RuntimeException("impossible case")
      }

    }

  implicit def CoproductFunctor3[F[_], G[_], H[_]](implicit FH: Functor[F], FT: Functor[({ type l[A] = G[A] :+: H[A] :+: CNil })#l]) =
    new Functor[({ type l[A] = F[A]:+: G[A]:+: H[A]:+: CNil })#l] {
      import Coproduct._
      def map[A, B](fa: F[A] :+: G[A] :+: H[A] :+: CNil)(f: A ⇒ B): F[B] :+: G[B] :+: H[B] :+: CNil = fa match {
        case Inl(h) ⇒ Coproduct[F[B] :+: G[B] :+: H[B] :+: CNil](FH.map(h)(f))
        case Inr(t) ⇒ FT.map(t)(f).extendLeft[F[B]]
        case _      ⇒ throw new RuntimeException("impossible case")
      }
    }
}
