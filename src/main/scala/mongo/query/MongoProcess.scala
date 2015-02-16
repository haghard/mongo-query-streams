package mongo.query

import scalaz.concurrent.Task
import scalaz.stream.Process._
import scalaz.stream.{ Channel, Process, Process1, Sink }

case class MongoProcess[T, A](channel: Channel[Task, T, Process[Task, A]]) {

  private def innerMap[B](f: Process[Task, A] ⇒ Process[Task, B]): MongoProcess[T, B] =
    MongoProcess(channel.map(r ⇒ r andThen (pt ⇒ pt.map(p ⇒ f(p)))))

  def map[B](f: A ⇒ B): MongoProcess[T, B] = innerMap(_.map(f))

  /** binds other MongoProcess to this MongoProcess **/
  def flatMap[B](f: A ⇒ MongoProcess[T, B]): MongoProcess[T, B] = MongoProcess {
    channel.map(
      (g: T ⇒ Task[Process[Task, A]]) ⇒ (task: T) ⇒
        g(task).map { pa ⇒
          pa.flatMap((a: A) ⇒
            f(a).channel.flatMap(h ⇒ eval(h(task)).flatMap(identity)))
        }
    )
  }

  /** applies [[scalaz.stream.Process.append]] on resulting stream **/
  def append[B >: A](p2: ⇒ Process[Task, B]): MongoProcess[T, B] = innerMap(_.append(p2))

  /**
   *
   * @param ch2
   * @tparam B
   * @return
   */
  def append[B >: A](ch2: MongoProcess[T, B]): MongoProcess[T, B] =
    MongoProcess(channel ++ ch2.channel)

  /**
   * applies [[scalaz.stream.Process.append]] on resulting stream
   * @param p2
   * @tparam B
   * @return
   */
  def ++[B >: A]()(p2: ⇒ Process[Task, B]): MongoProcess[T, B] = append(p2)

  /** alias for `append` of ChannelResult **/
  def ++[B >: A](ch2: MongoProcess[T, B]): MongoProcess[T, B] = append(ch2)

  /** applies [[scalaz.stream.Process.pipe]] on resulting stream **/
  def pipe[B](p2: Process1[A, B]): MongoProcess[T, B] = innerMap(_.pipe(p2))

  /** applies [[scalaz.stream.Process.pipe]] on resulting stream **/
  def |>[B](p2: Process1[A, B]): MongoProcess[T, B] = pipe(p2)

  /**
   *
   * @param p
   * @return
   */
  def to(p: Sink[Task, A]): MongoProcess[T, Unit] = innerMap(_ to p)

  /**
   *
   * @param p
   * @tparam B
   * @return
   */
  def through[B](p: Channel[Task, A, B]): MongoProcess[T, B] = innerMap(_ through (p))

  //def zipWith[B, C](p2: Process[Task, B])(f: (A, B) ⇒ C): MongoSource[T, C] = innerMap(_.zipWith(p2)(f))

  /** zips inner processes of ChannelResults */
  def zipWith[B, C](ch2: MongoProcess[T, B])(f: (A, B) ⇒ C): MongoProcess[T, C] = MongoProcess {
    val zipper: ((T ⇒ Task[Process[Task, A]], T ⇒ Task[Process[Task, B]]) ⇒ (T ⇒ Task[Process[Task, C]])) = {
      (fa, fb) ⇒
        (r: T) ⇒
          for {
            pa ← fa(r)
            pb ← fb(r)
          } yield (pa.zipWith(pb)(f))
    }

    channel.zipWith(ch2.channel)(zipper)
  }

  /*/** applies [[scalaz.stream.Process.zip]] on resulting stream **/
    def zip[B](p2: Process[Task, B]): MongoSource[T, (A, B)] = innerMap(_.zip(p2))
  */

  /** zips two channels together */
  def zip[B](ch2: MongoProcess[T, B]): MongoProcess[T, (A, B)] = zipWith(ch2)((a, b) ⇒ (a, b))
}