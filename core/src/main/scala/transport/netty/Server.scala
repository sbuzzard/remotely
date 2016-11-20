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

import fs2.{async,pipe,Strategy,Stream,Task}
import java.util.concurrent.Executors
import io.netty.channel._, socket.SocketChannel, nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.buffer.Unpooled
import io.netty.bootstrap.ServerBootstrap
import io.netty.handler.ssl.SslContext
import java.net.InetSocketAddress
import scodec.bits.BitVector

private[remotely] class NettyServer(handler: Handler,
                                    numBossThreads: Int,
                                    numWorkerThreads: Int,
                                    capabilities: Capabilities,
                                    logger: Monitoring,
                                    sslContext: Option[(SslContext,Boolean)])(implicit S: Strategy) {
  val bossThreadPool = new NioEventLoopGroup(numBossThreads, namedThreadFactory("nettyBoss"))
  val workerThreadPool = new NioEventLoopGroup(numWorkerThreads, namedThreadFactory("nettyWorker"))

  val bootstrap: ServerBootstrap = new ServerBootstrap()
    .group(bossThreadPool, workerThreadPool)
    .channel(classOf[NioServerSocketChannel])
    .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
    .childHandler(new ChannelInitializer[SocketChannel] {
                   override def initChannel(ch: SocketChannel): Unit = {
                     val pipe = ch.pipeline()
                     // add an SSL layer first iff we were constructed with an SslContext
                     sslContext.foreach { case (s,requireClientAuth) =>
                       logger.negotiating(None, "adding ssl", None)
                       val h = s.newHandler(ch.alloc())
                       h.engine.setNeedClientAuth(requireClientAuth)
                       pipe.addLast(h)
                     }
                     // add the rest of the stack
                     val _ = pipe.addLast(ChannelInitialize)
                   }
                 })


  /**
    * once a connection is negotiated, we send our capabilities string
    * to the client, which might look something like:
    *
    * OK: [Remotely 1.0]
    */
  @ChannelHandler.Sharable
  object ChannelInitialize extends ChannelInboundHandlerAdapter {
    override def channelRegistered(ctx: ChannelHandlerContext): Unit = {
      super.channelRegistered(ctx)
      logger.negotiating(Option(ctx.channel.remoteAddress), "channel connected", None)
      val encoded = Capabilities.capabilitiesCodec.encode(capabilities).require
      val fut = ctx.channel.writeAndFlush(Unpooled.wrappedBuffer(encoded.toByteArray))
      logger.negotiating(Option(ctx.channel.remoteAddress), "sending capabilities", None)
      val _ = fut.addListener(new ChannelFutureListener {
                                def operationComplete(cf: ChannelFuture): Unit = {
                                  if(cf.isSuccess) {
                                    logger.negotiating(Option(ctx.channel.remoteAddress), "sent capabilities", None)
                                    val p = ctx.pipeline()
                                    p.removeLast()
                                    p.addLast("deframe", new Deframe())
                                    p.addLast("enframe", Enframe)
                                    val _ = p.addLast("deframed handler", new ServerDeframedHandler(handler, logger) )
                                  } else {
                                    logger.negotiating(Option(ctx.channel.remoteAddress), s"failed to send capabilities", Option(cf.cause))
                                    shutdown()
                                  }
                                }
                              })
    }
  }

  def shutdown(): Unit = {
    bossThreadPool.shutdownGracefully()
    val _ = workerThreadPool.shutdownGracefully()
  }
}



object NettyServer {
  /**
    * start a netty server listening to the given address
    *
    * @param addr the address to bind to
    * @param handler the request handler
    * @param strategy the strategy used for processing incoming requests
    * @param bossThreads number of boss threads to create. These are
    * threads which accept incoming connection requests and assign
    * connections to a worker. If unspecified, the default of 2 will be used
    * @param workerThreads number of worker threads to create. If
    * unspecified the default of 2 * number of cores will be used
    * @param capabilities, the capabilities which will be sent to the client upon connection
    */
  def start(addr: InetSocketAddress,
            handler: Handler,
            strategy: Strategy = Strategy.fromExecutor(fixedNamedThreadPool("remotely-server")),
            bossThreads: Option[Int] = None,
            workerThreads: Option[Int] = None,
            capabilities: Capabilities = Capabilities.default,
            logger: Monitoring = Monitoring.empty,
            sslParameters: Option[SslParameters] = None): Task[Task[Unit]] = {
    SslParameters.toServerContext(sslParameters) map { ssl =>
      logger.negotiating(Some(addr), s"got ssl parameters: $ssl", None)
      val numBossThreads = bossThreads getOrElse 2
      val numWorkerThreads = workerThreads getOrElse Runtime.getRuntime.availableProcessors.max(4)

      val server = new NettyServer(handler, numBossThreads, numWorkerThreads, capabilities, logger, ssl.map(_ -> sslParameters.fold(true)(p => p.requireClientAuth)))(strategy)
      val b = server.bootstrap

      logger.negotiating(Some(addr), s"about to bind", None)
      val channel = b.bind(addr)
      logger.negotiating(Some(addr), s"bound", None)
      Task.unforkedAsync[Unit] { cb =>
        val _ = channel.addListener(new ChannelFutureListener {
                                      override def operationComplete(cf: ChannelFuture): Unit = {
                                        server.shutdown()
                                        if(cf.isSuccess) {
                                          cf.channel.close().awaitUninterruptibly()
                                        }
                                        cb(Right(()))
                                      }
                                    })
      }
    }
  }
}

/**
  * We take the bits coming to us, which have been partitioned for us
  * by FrameDecoder.
  *
  * every time we see a boundary, we close one stream, open a new
  * one, and setup a new outgoing stream back to the client.
  */
class ServerDeframedHandler(handler: Handler, logger: Monitoring)(implicit S: Strategy) extends SimpleChannelInboundHandler[Framed] {

  @volatile private var queue: Option[async.mutable.Queue[Task, Option[Either[Throwable, BitVector]]]] = None

  //
  //  We've getting some bits, make sure we have a queue open if these
  //  bits are part of a new request.
  //
  //  if we don't have a queue open, we need to:
  //    - open a queue
  //    - get the queue's output stream,
  //    - apply the handler to the stream to get a response stream
  //    - evalMap that stream onto the side effecty "Context" to write
  //      bytes back to to the client (i hate the word context)
  //
  private def ensureQueue(ctx: ChannelHandlerContext): Task[async.mutable.Queue[Task, Option[Either[Throwable, BitVector]]]] = {
    val newQueue = for {
      _ <- Task.delay(logger.negotiating(Option(ctx.channel.remoteAddress), "creating queue", None))
      q <- async.unboundedQueue[Task, Option[Either[Throwable, BitVector]]]
      framed = handler(q.dequeue.through(pipe.unNoneTerminate andThen unLeftFail)).map(Bits(_)).append(Stream.emit(EOS))
      _ <- Task.start(framed.evalMap { b => Task.delay(ctx.write(b)) }.run flatMap { _ => Task.delay(ctx.flush) })
      _ <- Task.delay(queue = Some(q))
    } yield q
    Task.delay(queue) flatMap { _.fold(newQueue) { Task.now } }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable): Unit = (for {
    _ <- Task.delay(ctx.channel.close()).attempt
    _ <- Task.delay(logger.negotiating(Option(ctx.channel.remoteAddress), "shutting down queue on failure", None))
    q <- Task.delay {
      val q = queue
      queue = None
      q
    }
    _ <- q.fold(Task.now(())) { _.enqueue1(Some(Left(e))) }
  } yield ()).unsafeRun

  // we've seen the end of the input, close the queue writing to the input stream
  private def closeQueue(ctx: ChannelHandlerContext): Task[Unit] = for {
    _ <- Task.delay(logger.negotiating(Option(ctx.channel.remoteAddress), "shutting down queue", None))
    q <- Task.delay {
      val q = queue
      queue = None
      q
    }
    _ <- q.fold(Task.now(())) { _.enqueue1(Option.empty[Either[Throwable, BitVector]]) }
  } yield ()

  override def channelRead0(ctx: ChannelHandlerContext, f: Framed): Unit = (f match {
    case Bits(bv) => for {
      q <- ensureQueue(ctx)
      _ <- q.enqueue1(Some(Right(bv)))
    } yield ()
    case EOS =>
      closeQueue(ctx)
  }).unsafeRun
}

