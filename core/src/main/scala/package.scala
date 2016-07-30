//: ----------------------------------------------------------------------------
//: Copyright (C) 2014 Verizon.  All Rights Reserved.
//:
//:   Licensed under the Apache License, Version 2.0 (the "License");
//:   you may not use this file except in compliance with the License.
//:   You may obtain a copy of the License at
//:
//:       http://www.apache.org/licenses/LICENSE-2.0
//:
//:   Unless required by applicable law or agreed to in writing, software
//:   distributed under the License is distributed on an "AS IS" BASIS,
//:   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//:   See the License for the specific language governing permissions and
//:   limitations under the License.
//:
//: ----------------------------------------------------------------------------


package object remotely {
  import cats.Monad
  import cats.data.Xor
  import cats.implicits._
  import fs2._
  import fs2.interop.cats._
  import fs2.util.{Catchable,Functor => Fs2Functor}
  import java.util.concurrent.{Executors,ExecutorService,ThreadFactory}
  import java.util.concurrent.atomic.AtomicInteger
  import scala.concurrent.duration._
  import scala.reflect.runtime.universe.TypeTag
  import scodec.{Attempt,Err}
  import scodec.bits.{BitVector,ByteVector}
  import utils._

/**
  * Represents the logic of a connection handler, a function
  * from a stream of bytes to a stream of bytes, which will
  * be sent back to the client. The connection will be closed
  * when the returned stream terminates.
  *
  * NB: This type cannot represent certain kinds of 'coroutine'
  * client/server interactions, where the server awaits a response
  * to a particular packet sent before continuing.
  */
  type Handler = Stream[Task,BitVector] => Stream[Task,BitVector]

  /**
   * Evaluate the given remote expression at the
   * specified endpoint, and get back the result.
   * This function is completely pure - no network
   * activity occurs until the returned `Response` is
   * run.
   *
   * The `Monitoring` instance is notified of each request.
   */
  def evaluate[A:scodec.Codec:TypeTag](e: Endpoint, M: Monitoring = Monitoring.empty)(r: Remote[A]): Response[A] =
  Response.scope { Response { ctx => // push a fresh ID onto the call stack
    val refs = Remote.refs(r)

    def reportErrors[R](startNanos: Long)(t: Task[R]): Task[R] =
      t.bestEffortOnFinish {
        case Some(e) =>
          Task.delay {
            M.handled(ctx, r, refs, Xor.left(e), Duration.fromNanos(System.nanoTime - startNanos))
          }
        case None => Task.now(())
      }

    Task.delay { System.nanoTime } flatMap { start =>
      for {
        conn <- e.get
        reqBits <- codecs.encodeRequest(r, ctx).toTask
        respBytes <- reportErrors(start) {
          val reqBytestream = Stream.emit(reqBits)
          val bytes = fullyRead(conn(reqBytestream))
          bytes
        }
        resp <- reportErrors(start) { codecs.responseDecoder[A].complete.decode(respBytes).map(_.value).toTask }
        result <- resp.fold(
          { e =>
            val ex = ServerException(e)
            val delta = System.nanoTime - start
            M.handled(ctx, r, Remote.refs(r), Xor.left(ex), Duration.fromNanos(delta))
            Task.fail(ex)
          },
          { a =>
            val delta = System.nanoTime - start
            M.handled(ctx, r, refs, Xor.right(a), Duration.fromNanos(delta))
            Task.now(a)
          }
        )
      } yield result
    }
  }}

  implicit class EnrichedThrowable(val t: Throwable) extends AnyVal {
    def safeMessage: String = Option(t.getMessage).getOrElse(t.toString)
  }
  implicit class RemotelyEnrichedAttempt[A](val self: Attempt[A]) extends AnyVal {
    def toTask: Task[A] = toTask(err => new IllegalArgumentException(err.messageWithContext))
    def toTask(f: Err => Throwable): Task[A] = self.fold(e => Task.fail(f(e)), Task.now)
    def toXor: Err Xor A = self.fold(Xor.left, Xor.right)
  }

  def once[F[_], A](s: Stream[F, A]): Stream[F, A] = s.through(pipe.covary[F, A, A](echo1)).through(pipe.take(1))
  def runLast[F[_]: Fs2Functor: Catchable, A](s: Stream[F, A]): F[Option[A]] = s.runLog.map(_.lastOption)
  def runOnceLast[F[_]: Fs2Functor: Catchable, A](s: Stream[F, A]): F[Option[A]] = runLast(once(s))
  def iterate[A](start: A)(f: A => A): Stream[Pure, A] = Stream.emit(start) ++ iterate(f(start))(f)
  def echo1[A]: Pipe[Pure, A, A] = _.pull(Pull.echo1)

  private[remotely] def fullyRead(s: Stream[Task,BitVector]): Task[BitVector] = s.runFold(BitVector.empty)((a, b) => a ++ b)

  private[remotely] def fixedNamedThreadPool(name: String): ExecutorService =
    Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors.max(4), namedThreadFactory(name))

  private[remotely] def namedThreadFactory(name: String): ThreadFactory = new ThreadFactory {
    val num = new AtomicInteger(1)
    def newThread(runnable: Runnable) = {
      val t = new Thread(runnable, s"$name - ${num.getAndIncrement}")
      t.setDaemon(true)
      t
    }
  }

  private[remotely] implicit class EnrichedTaskForRemotely[A](val self: Task[A]) extends AnyVal {
    def bestEffortOnFinish(f: Option[Throwable] => Task[Unit]): Task[A] = self.attempt flatMap { r =>
      f(r.left.toOption).attempt flatMap { _ => r.fold(Task.fail, Task.now) }
    }
  }
}
