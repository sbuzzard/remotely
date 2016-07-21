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
package transport.netty

import cats.data.Xor

import fs2.{async,pipe,Strategy,Stream,Task}

import java.net.InetSocketAddress
import org.apache.commons.pool2.impl.GenericObjectPool
import io.netty.channel.{Channel,ChannelFuture,ChannelHandlerContext,ChannelFutureListener}
import scodec.bits.BitVector

class NettyTransport(val pool: GenericObjectPool[Channel])(implicit S: Strategy) extends Handler {
  import NettyTransport._
  def apply(toServer: Stream[Task, BitVector]): Stream[Task, BitVector] = {
    case class QueueableChannel(q: async.mutable.Queue[Task, Option[BitVector]], c: Channel)
    val openQueueableChannel = async.unboundedQueue[Task, Option[BitVector]].async(NettyTransport.clientQueuePool) map { q =>
      val c = pool.borrowObject
      c.pipeline.addLast("clientDeframe", new ClientDeframedHandler(q))
      QueueableChannel(q, c)
    }
    Stream.eval(openQueueableChannel).flatMap { qc =>
      val toFrame = toServer.map(Bits(_)) ++ Stream.emit(EOS)
      val writeBytes: Task[Unit] = toFrame.evalMap(write(qc.c)).run flatMap { _ => Task.delay { val _ = qc.c.flush } }
      Stream.eval(writeBytes.async(S)).flatMap(_ => qc.q.dequeue.through(pipe.unNoneTerminate)).append(Stream.eval_(Task.delay(pool.returnObject(qc.c)))).onError { t =>
        pool.invalidateObject(qc.c)
        Stream.fail(t)
      }
    }
  }

  def shutdown: Task[Unit] = Task.delay {
    pool.clear()
    pool.close()
    val _ = pool.getFactory().asInstanceOf[NettyConnectionPool].workerThreadPool.shutdownGracefully()
  }
}


object NettyTransport {
  private val clientQueuePool = Strategy.fromExecutor(fixedNamedThreadPool("client-queue-pool"))
  def evalCF(cf: ChannelFuture)(implicit S: Strategy): Task[Unit] = Task.unforkedAsync { (cb: Either[Throwable, Unit] => Unit) =>
    cf.addListener(new ChannelFutureListener {
      def operationComplete(cf: ChannelFuture): Unit = if (cf.isSuccess) cb(Right(())) else cb(Left(cf.cause))
    })
    ()
  }

  def write(c: Channel)(frame: Framed)(implicit S: Strategy): Task[Unit] = evalCF(c.writeAndFlush(frame))

  def single(host: InetSocketAddress,
             expectedSigs: Set[Signature] = Set.empty,
             workerThreads: Option[Int] = None,
             monitoring: Monitoring = Monitoring.empty,
             sslParams: Option[SslParameters] = None,
             channelPoolConfig: Option[ChannelPoolConfig] = None)(implicit S: Strategy): Task[NettyTransport] =
    NettyConnectionPool.default(Stream.constant(host), expectedSigs, workerThreads, monitoring, sslParams, channelPoolConfig).map(new NettyTransport(_))
}
