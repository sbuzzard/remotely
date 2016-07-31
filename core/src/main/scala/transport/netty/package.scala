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

package remotely.transport

import fs2._

import _root_.io.netty.channel._
import _root_.io.netty.channel.pool._
import _root_.io.netty.util.concurrent._

import scala.language.higherKinds

package object netty {

  def unLeftFail[F[_], I]: Stream[F, Either[Throwable, I]] => Stream[F, I] = _ repeatPull {
    _.receive {
      case hd #: tl =>
        val failureMaybe = hd.toVector.collectFirst { case Left(t) => t }
        def success = {
          val out = Chunk.indexedSeq(hd.toVector.collect { case Right(i) => i })
          if (out.size == hd.size) Pull.output(out) as tl else if (out.isEmpty) Pull.done else Pull.output(out) >> Pull.done
        }
        failureMaybe.fold(success)(Pull.fail)
    }
  }

  def unsafeRunAsyncChannelFuture(ch: Channel, task: Task[Unit]): ChannelFuture = {
    val promise = new DefaultChannelPromise(ch)
    task.unsafeRunAsync { cb =>
      cb.fold(promise.setFailure, _ => promise.setSuccess())
      ()
    }
    promise
  }

  def fromNettyChannelFuture(fut: => ChannelFuture)(implicit S: Strategy): Task[Channel] = Task.async[Channel] { cb =>
    fut.addListener(new ChannelFutureListener {
      def operationComplete(cf: ChannelFuture): Unit = {
        if (cf.isSuccess) cb(Right(cf.channel)) else cb(Left(cf.cause))
        ()
      }
    })
    ()
  }

  def fromNettyFuture[A](fut: => Future[A])(implicit S: Strategy): Task[A] = Task.async[A] { cb =>
    fut.addListener(new FutureListener[A] {
      def operationComplete(f: Future[A]): Unit = {
        if (f.isSuccess) cb(Right(f.getNow)) else cb(Left(f.cause))
        ()
      }
    })
    ()
  }
}
