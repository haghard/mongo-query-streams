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

import join.DBModule
import rx.lang.scala.Observable
import join.process.MongoProcessStream
import join.observable.MongoObservableStream
import cassandra.{ CassandraObservableStream, CassandraProcessStream }

package object joinG {

  trait JoinerG[T <: DBModule] {
    def join[A, B, C](l: T#DBStream[A])(relation: A ⇒ T#DBStream[B])(f: (A, B) ⇒ C): T#DBStream[C]
  }

  object JoinerG {

    implicit object MongoP extends JoinerG[MongoProcessStream] {
      def join[A, B, C](l: MongoProcessStream#DBStream[A])(relation: A ⇒ MongoProcessStream#DBStream[B])(f: (A, B) ⇒ C): MongoProcessStream#DBStream[C] =
        for { id ← l; rs ← relation(id) |> scalaz.stream.process1.lift(f(id, _)) } yield rs
    }

    implicit object MongoO extends JoinerG[MongoObservableStream] {
      override def join[A, B, C](l: MongoObservableStream#DBStream[A])(relation: A ⇒ MongoObservableStream#DBStream[B])(f: (A, B) ⇒ C): MongoObservableStream#DBStream[C] =
        for { id ← l; rs ← relation(id).map(f(id, _)) } yield rs
    }

    implicit object CassandraP extends JoinerG[CassandraProcessStream] {
      override def join[A, B, C](l: CassandraProcessStream#DBStream[A])(relation: A ⇒ CassandraProcessStream#DBStream[B])(f: (A, B) ⇒ C): CassandraProcessStream#DBStream[C] =
        for { id ← l; rs ← relation(id) |> scalaz.stream.process1.lift(f(id, _)) } yield rs
    }

    implicit object CassandraO extends JoinerG[CassandraObservableStream] {
      override def join[A, B, C](l: Observable[A])(relation: (A) ⇒ Observable[B])(f: (A, B) ⇒ C): Observable[C] =
        for { id ← l; rs ← relation(id).map(f(id, _)) } yield rs
    }

    def apply[T <: DBModule: JoinerG]: JoinerG[T] = implicitly[JoinerG[T]]
  }
}