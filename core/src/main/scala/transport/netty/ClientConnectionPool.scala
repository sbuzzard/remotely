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

import fs2.{Strategy,Stream,Task}

import java.net.InetSocketAddress
import java.util.concurrent.{Executors, ThreadFactory}
import io.netty.util.concurrent.{Future, GenericFutureListener}
import io.netty.channel._, socket._
import io.netty.channel.nio._
import io.netty.channel.pool._
import io.netty.bootstrap.Bootstrap
import io.netty.channel.{Channel, ChannelFuture, ChannelFutureListener,ChannelHandlerContext,SimpleChannelInboundHandler}
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.{Delimiters,DelimiterBasedFrameDecoder}
import io.netty.handler.ssl.SslContext
import remotely.utils._
import remotely.Response.Context
import scodec.{Attempt, Err}
import scodec.bits.BitVector
import io.netty.buffer.ByteBuf

object NettyConnectionPool {
  def default(host: InetSocketAddress,
              expectedSigs: Set[Signature] = Set.empty,
              workerThreads: Option[Int] = None,
              monitoring: Monitoring = Monitoring.empty,
              sslParams: Option[SslParameters])(implicit S: Strategy): Task[NettyConnectionPool] = {
    def genWorkerThreadPool = Task.delay(new NioEventLoopGroup(workerThreads getOrElse Runtime.getRuntime.availableProcessors.max(4), namedThreadFactory("nettyWorker")))
    def genBootstrap(workerThreadPool: NioEventLoopGroup) = Task.delay {
      val bootstrap = new Bootstrap()
      bootstrap.option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
      bootstrap.group(workerThreadPool)
      bootstrap.channel(classOf[NioSocketChannel])
      bootstrap
    }
    for {
      workerThreadPool <- genWorkerThreadPool
      bootstrap <- genBootstrap(workerThreadPool)
      ssl <- SslParameters.toClientContext(sslParams)
    } yield new NettyConnectionPool(host, workerThreadPool, bootstrap, expectedSigs, monitoring, ssl)
  }
}

case class IncompatibleServer(msg: String) extends Throwable(msg)

class NettyConnectionPoolHandler extends AbstractChannelPoolHandler {
  override def channelCreated(ch: Channel): Unit = ()
  override def channelReleased(ch: Channel): Unit = {
    ch.pipeline.remove(classOf[ClientDeframedHandler])
    ()
  }
}

class NettyConnectionPool(host: InetSocketAddress,
                          val workerThreadPool: NioEventLoopGroup,
                          bootstrap: Bootstrap,
                          expectedSigs: Set[Signature],
                          M: Monitoring,
                          sslContext: Option[SslContext])(implicit S: Strategy) extends SimpleChannelPool(bootstrap, new NettyConnectionPoolHandler) {

  val validateCapabilities: ((Capabilities,Channel)) => Task[Channel] = {
    case (capabilties, channel) =>

      val pipe    = channel.pipeline()
      val missing = Capabilities.required -- capabilties.capabilities

      if(missing.isEmpty) {
        Task.delay {
          pipe.removeLast()
          pipe.addLast("enframe", Enframe)
          pipe.addLast("deframe", new Deframe)
          channel
        }
      }
      else {

        def error = IncompatibleServer(
          s"server missing required capabilities: $missing"
        )

        Task.unforkedAsync[Channel] { cb =>

          pipe.removeLast()

          val _ = channel.close().addListener(
            new ChannelFutureListener {
              def operationComplete(cf: ChannelFuture) =
                if(cf.isSuccess) cb(Right(cf.channel)) else cb(Left(cf.cause))
            }
          )

        } flatMap { _ => Task.fail(error) }
      }
  }

  /**
    * This is an upstream handler that sits in the client's pipeline
    * during connection negotiation.
    *
    * It is put into the pipeline initially, we then make a call to
    * the server to ask for the descriptions of all the functions that
    * the server provides. when we receive the response, this handler
    * checks that all of the expectedSigs passed to the constructor
    * are present in the server response.
    *
    * The state of this negotiation is captured by the `valid` Task,
    * which is asynchronously updated when the response is recieved
    */
  class ClientNegotiateDescription(channel: Channel,
                                   expectedSigs: Set[Signature],
                                   host: InetSocketAddress) extends SimpleChannelInboundHandler[Framed] {

    M.negotiating(Some(host), "description negotiation begin", None)


    // the callback which will fulfil the valid task
    @volatile private[this] var cb: Either[Throwable, Channel] => Unit = Function.const(())

    val valid: Task[Channel] = Task.unforkedAsync[Channel] { cb =>
      this.cb = cb
    }

    // here we accumulate bits as they arrive, we keep accumulating
    // until the handler below us signals the end of stream by
    // emitting a EOS
    @volatile private[this] var bits = BitVector.empty

    // negotiation failed. fulfil the callback negatively, and remove
    // ourselves from the pipeline
    private[this] def fail(msg: String): Unit = {
      val err = IncompatibleServer(msg)

      val _ = Xor.catchNonFatal { channel.pipeline().removeLast() }

      M.negotiating(Some(host), "description", Some(err))
      cb(Left(err))
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, ee: Throwable): Unit = {
      ee.printStackTrace()
      fail(ee.safeMessage)
    }

    // negotiation succeeded, fulfill the callback positively, and
    // remove ourselves from the pipeline
    private[this] def success(): Unit = {
      val pipe = channel.pipeline()
      pipe.removeLast()
      M.negotiating(Some(host), "description", None)
      cb(Right(channel))
    }

    override def channelRead0(ctx: ChannelHandlerContext, f: Framed): Unit = {
      f match {
        case Bits(bv) =>
          bits = bits ++ bv
        case EOS =>
          M.negotiating(Some(host), "got end of description response", None)
          val signatureDecoding: Attempt[Unit] = for {
            resp <- codecs.responseDecoder[List[Signature]](codecs.list(Signature.signatureCodec)).complete.decode(bits).map(_.value)
          }  yield resp.fold(e => fail(s"error processing description response: $e"),
                             serverSigs => {
                               val missing = expectedSigs -- serverSigs
                               if(missing.isEmpty) {
                                 success()
                               } else {
                                 fail(s"server is missing required signatures: ${missing.map(_.tag)}}")
                               }
                             })
          signatureDecoding fold (
            e => M.negotiating(Some(host), "error processing description", Some(e)),
            _ => M.negotiating(Some(host), "finished processing response", None)
          )
      }
    }


    private[this] val pipe = channel.pipeline()
    pipe.addLast("negotiateDescription", this)

    //
    // and then they did some investigation and realized
    // <gasp>
    // THE CALL WAS COMING FROM INSIDE THE HOUSE
    /**
      * a Task which actually makes a request to the server for the
      * description of server supported functions
      */
    private[this] val requestDescription: Task[Unit] = for {
      bits <- codecs.encodeRequest(Remote.ref[List[Signature]]("describe"), Context.empty).toTask
      _ <- NettyTransport.evalCF(channel.write(Bits(bits)))
      _ <- NettyTransport.evalCF(channel.writeAndFlush(EOS))
      _ = M.negotiating(Some(host), "sending describe request", None)
    } yield ()

    // actually make the request for the description
    requestDescription.unsafeRunAsync {
      case Right(bits) => ()
      case Left(x) => fail("error requesting server description: " + x)
    }
  }

  /**
    * pipeline handler which expects the Capabilities string which the
    * server sends as soon as we connect. this will check that all the
    * required capabilities are present. if so, it will replace itself
    * with the ClientNegotiateDescription handler to do the next part
    * of the negotiation.
    */
  class ClientNegotiateCapabilities extends DelimiterBasedFrameDecoder(1000,Delimiters.lineDelimiter():_*) {

    // callback which fulfills the capabilities Task
    @volatile private[this] var cb: Either[Throwable, (Capabilities,Channel)] => Unit = Function.const(())

    val capabilities: Task[(Capabilities,Channel)] = Task.unforkedAsync[(Capabilities, Channel)] { cb =>
      this.cb = cb
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, ee: Throwable): Unit = {
      ee.printStackTrace()
      cb(Left(ee))
    }

    override def decode(ctx: ChannelHandlerContext, buffer: ByteBuf): Object = {
      val bytes = new Array[Byte](buffer.readableBytes)
      buffer.readBytes(bytes)
      val str = new String(bytes, "UTF-8")
      M.negotiating(None, s"received capabilities string: $str", None)
      val r = Capabilities.parseHelloString(str).toXor.bimap(
        (e: Err) => new IllegalArgumentException(e.message),
        (cap: Capabilities) => (cap,ctx.channel)
      )
      cb(r.toEither)
      r
    }
  }

  override def connectChannel(bs: Bootstrap): ChannelFuture = {
    val negotiateCapable = new ClientNegotiateCapabilities()
    val chanFut = {
      val init = new ChannelInitializer[SocketChannel] {
        def initChannel(ch: SocketChannel): Unit = {

          val pipe = ch.pipeline

          // add an SSL layer first iff we were constructed with an SslContext, foreach like a unit boss

          sslContext.foreach { s =>

            val sh = s.newHandler(ch.alloc(), host.getAddress.getHostAddress, host.getPort)

            sh.handshakeFuture().addListener(new GenericFutureListener[Future[Channel]] {
              def operationComplete(future: Future[Channel]): Unit = {
                // avoid negotiation when ssl fails
                if(!future.isSuccess) pipe.remove(negotiateCapable)
                ()
              }
            })
            pipe.addLast(sh)
          }
          val effect = pipe.addLast(negotiateCapable)
        }
      }
      val h = bs.handler(init)
      bs.connect(host)
    }
    val task = for {
      chan <- fromNettyChannelFuture(chanFut)
      _ = M.negotiating(Some(host), "channel selected", None)
      capable <- negotiateCapable.capabilities
      _ = M.negotiating(Some(host), "capabilities received", None)
      _ <- validateCapabilities(capable)
      _ = M.negotiating(Some(host), "capabilities valid", None)
      _ <- if (expectedSigs.isEmpty) Task.now(chan) else new ClientNegotiateDescription(chan, expectedSigs, host).valid
      _ = M.negotiating(Some(host), "description valid", None)
    } yield ()
    unsafeRunAsyncChannelFuture(chanFut.channel, task)
  }
}
