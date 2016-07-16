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

package remotely

import cats.implicits._

import fs2._
import fs2.interop.cats._
import fs2.util.{Functor => Fs2Functor, _}

import java.net.{InetSocketAddress,Socket,URL}
import javax.net.ssl.SSLEngine
import scodec.bits.BitVector
import scala.concurrent.duration._

/**
 * A 'logical' endpoint for some service, represented
 * by a possibly rotating stream of `Transport`s.
 */
case class Endpoint(connections: Stream[Task,Handler]) {
  def get: Task[Handler] = runOnceLast(connections).flatMap {
    case None => Task.fail(new Exception("No available connections"))
    case Some(a) => Task.now(a)
  }


  /**
    * Adds a circuit-breaker to this endpoint that "opens" (fails fast) after
    * `maxErrors` consecutive failures, and attempts a connection again
    * when `timeout` has passed.
    */
  def circuitBroken(timeout: Duration, maxErrors: Int): Endpoint =
    Endpoint(connections.map(c => (bs: Stream[Task,BitVector]) => c(bs).translate(CircuitBreaker(timeout, maxErrors).transform)))
}

object Endpoint {

  def empty: Endpoint = Endpoint(Stream.empty)
  def single(transport: Handler): Endpoint = Endpoint(Stream.constant(transport).covary[Task])

  /**
    * If a connection in an endpoint fails, then attempt the same call to the next
    * endpoint, but only if `timeout` has not passed AND we didn't fail in a
    * "committed state", i.e. we haven't received any bytes.
    */

  def failoverChain(timeout: Duration, es: Stream[Task, Endpoint]): Endpoint = Endpoint(
    transpose(es.map(_.connections)).flatMap { cs =>
      cs.through(
        pipe.reduce {
          (c1, c2) => bs => time(c1(bs).attempt) flatMap {
            case (d, Left(e)) => if (timeout - d > 0.milliseconds) c2(bs) else Stream.fail(new Exception(s"Failover chain timed out after $timeout"))
            case (d, Right(x)) => Stream.emit(x)
          }
        }
      )
    }
  )

  /**
    * An endpoint backed by a (static) pool of other endpoints.
    * Each endpoint has its own circuit-breaker, and fails over to all the others
    * on failure.
    */
  def uber(maxWait: Duration,
           circuitOpenTime: Duration,
           maxErrors: Int,
           es: Stream[Task, Endpoint]): Endpoint = {
    Endpoint(raceHandlerPool(permutations(es).map(ps => failoverChain(maxWait, ps.map(_.circuitBroken(circuitOpenTime, maxErrors))).connections)))
  }

  /**
    * Produce a stream of all the permutations of the given stream.
    */
  private[remotely] def permutations[F[_]: Catchable, A](s: Stream[F, A]): Stream[F, Stream[F, A]] = {
    val xs = iterate(0)(_ + 1) zip s
    for {
      b <- Stream.eval(isEmpty(xs))
      r <- if (b) Stream.emit(xs) else for {
        x <- xs
        ps <- permutations(xs through pipe.delete { case (i, v) => i == x._1 })
      } yield Stream.emit(x) ++ ps
    } yield r.map(_._2)
  }

  /**
    * Transpose a stream of streams to emit all their first elements, then all their second
    * elements, and so on.
    */
  private[remotely] def transpose[F[_]: Catchable, A](as: Stream[F, Stream[F, A]]): Stream[F, Stream[F, A]] =
    Stream.emit(as.flatMap(_.take(1))) ++ Stream.eval(isEmpty(as.flatMap(_.drop(1)))).flatMap(b =>
      if(b) Stream.empty else transpose(as.map(_.drop(1))))

  /**
    * Returns true if the given process never emits.
    */
  private[remotely] def isEmpty[F[_]: Catchable, O](p: Stream[F, O])(implicit F: Fs2Functor[F]): F[Boolean] =
    F.map(runOnceLast(p.map(_ => false)))(_.getOrElse(true))

  private def time[A](stream: Stream[Task, A]): Stream[Task, (Duration, A)] = for {
    t1 <- Stream.eval(Task.delay(System.currentTimeMillis))
    a  <- stream
    t2 <- Stream.eval(Task.delay(System.currentTimeMillis))
  } yield ((t2 - t1).milliseconds, a)

  private object raceHandlerPool {
    implicit val S: Strategy = Strategy.fromExecutor(fixedNamedThreadPool("remotely-client-pool"))
    def apply(pool: Stream[Task, Stream[Task, Handler]]): Stream[Task, Handler] = concurrent.join(1024)(pool)
  }
}


