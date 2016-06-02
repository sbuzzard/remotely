//: ----------------------------------------------------------------------------
//: Copyright (C) 2015 Verizon.  All Rights Reserved.
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

import cats.data.OptionT
import cats.implicits._
import fs2.{Stream,Task}
import fs2.interop.cats._

import org.scalatest.matchers.{Matcher,MatchResult}
import org.scalatest.{FlatSpec,Matchers,BeforeAndAfterAll}
import java.io.File
import java.nio.file.Paths
import Response.Context
import transport.netty._
import codecs._

class SSLSpec extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {


  behavior of "Netty SSL Server"

  val pems = List("CA.pem", "client_cert.pem", "server_cert.pem")
  val keys = List("client_key.pk8", "server_key.pk8")

  val caCert = new File(getClass.getResource("/ssl-testing/CA.pem").getFile)

  val clientCert = new File(getClass.getResource("/ssl-testing/client_cert.pem").getFile)
  val serverCert = new File(getClass.getResource("/ssl-testing/server_cert.pem").getFile)

  val clientKey = new File(getClass.getResource("/ssl-testing/client_key.pk8").getFile)
  val serverKey = new File(getClass.getResource("/ssl-testing/server_key.pk8").getFile)

  val serverRequiringAuthParameters = SslParameters(Some(caCert),
                                                    Some(serverCert),
                                                    Some(serverKey),
                                                    None,
                                                    None,
                                                    None,
                                                    true)
  val serverNoAuthParameters = SslParameters(Some(caCert),
                                             Some(serverCert),
                                             Some(serverKey),
                                             None,
                                             None,
                                             None,
                                             true)

  val clientAuthParameters = SslParameters(Some(caCert),
                                           Some(clientCert),
                                           Some(clientKey),
                                           None,
                                           None,
                                           None,
                                           true)

  val clientNoAuthParameters = SslParameters(Some(caCert),
                                             Some(clientCert),
                                             Some(clientKey),
                                             None,
                                             None,
                                             None,
                                             true)


  val addr = new java.net.InetSocketAddress("localhost", 9101)
  val server = new TestServer

  it should "be able to do client authentication" in {
    import remotely.Remote.implicits._

    val shutdown = server.environment.serve(addr,
                                            monitoring = Monitoring.consoleLogger("SSLSpec-server"),
                                            sslParams = Some(serverRequiringAuthParameters)).unsafeRun
    val transport = NettyTransport.single(addr,
                                          monitoring = Monitoring.consoleLogger("SSLSpec-client"),
                                          sslParams = Some(clientAuthParameters)).unsafeRun

    val endpoint: Endpoint = Endpoint.single(transport)

    try {
      val fact: Int = evaluate(endpoint, Monitoring.consoleLogger())(Client.factorial(10)).apply(Context.empty).unsafeRun
      fact should be (100)
    } finally {
      shutdown.unsafeRun
      transport.shutdown.unsafeRun
    }
  }

  it should "reject non auth clients when auth is required" in {
    import remotely.Remote.implicits._

    val shutdown = server.environment.serve(addr,
                                            monitoring = Monitoring.consoleLogger("SSLSpec-server"),
                                            sslParams = Some(serverRequiringAuthParameters)).unsafeRun
    val transport = NettyTransport.single(addr,
                                          monitoring = Monitoring.consoleLogger("SSLSpec-client"),
                                          sslParams = Some(clientNoAuthParameters)).unsafeRun

    try {
      val endpoint: Endpoint = Endpoint.single(transport)

      val fact: Int = evaluate(endpoint, Monitoring.consoleLogger())(Client.factorial(10)).apply(Context.empty).unsafeRun
      fact should be (100)
    } finally {
      shutdown.unsafeRun
      transport.shutdown.unsafeRun
    }
  }

  it should "work without auth" in {
    import remotely.Remote.implicits._

    val shutdown = server.environment.serve(addr,
                                            monitoring = Monitoring.consoleLogger("SSLSpec-server"),
                                            sslParams = Some(serverNoAuthParameters)).unsafeRun
    val transport = NettyTransport.single(addr,
                                          monitoring = Monitoring.consoleLogger("SSLSpec-client"),
                                          sslParams = Some(clientNoAuthParameters)).unsafeRun

    try {
      val endpoint: Endpoint = Endpoint.single(transport)

      val fact: Int = evaluate(endpoint, Monitoring.consoleLogger())(Client.factorial(10)).apply(Context.empty).unsafeRun
      fact should be (100)
    } finally {
      shutdown.unsafeRun
      transport.shutdown.unsafeRun
    }
  }

  it should "work with with auth client and no-auth server" in {
    import remotely.Remote.implicits._

    val shutdown = server.environment.serve(addr,
                                            monitoring = Monitoring.consoleLogger("SSLSpec-server"),
                                            sslParams = Some(serverNoAuthParameters)).unsafeRun
    val transport = NettyTransport.single(addr,
                                          monitoring = Monitoring.consoleLogger("SSLSpec-client"),
                                          sslParams = Some(clientAuthParameters)).unsafeRun

    try {
      val endpoint: Endpoint = Endpoint.single(transport)

      val fact: Int = evaluate(endpoint, Monitoring.consoleLogger())(Client.factorial(10)).apply(Context.empty).unsafeRun
      fact should be (100)
    } finally {
      shutdown.unsafeRun
      transport.shutdown.unsafeRun
    }
  }

  it should "reject a non-ssl server from an ssl client" in {
    import remotely.Remote.implicits._

    val shutdown = server.environment.serve(addr,
                                            monitoring = Monitoring.consoleLogger("SSLSpec-server"),
                                            sslParams = None).unsafeRun
    val transport = NettyTransport.single(addr,
                                          monitoring = Monitoring.consoleLogger("SSLSpec-client"),
                                          sslParams = Some(clientNoAuthParameters)).unsafeRun

    try {
      val endpoint: Endpoint = Endpoint.single(transport)

      val fact = evaluate(endpoint, Monitoring.consoleLogger())(Client.factorial(10)).apply(Context.empty)

      an[io.netty.handler.ssl.NotSslRecordException] should be thrownBy (
        try {
          val _ = fact.unsafeRun
        } catch {
          case t: io.netty.handler.ssl.NotSslRecordException =>
            throw t
          case t: Throwable =>
            t.printStackTrace
            throw t
        })
    } finally {
      shutdown.unsafeRun
      transport.shutdown.unsafeRun
    }
  }

  // ignored for now becuase it hangs the client, ths needs to be fixed
  ignore should "give a good error when a non-ssl client tries to connect to an ssl server" in {
    import remotely.Remote.implicits._

    val shutdown = server.environment.serve(addr,
                                            monitoring = Monitoring.consoleLogger("SSLSpec-server"),
                                            sslParams = Some(serverNoAuthParameters)).unsafeRun
    val transport = NettyTransport.single(addr,
                                          monitoring = Monitoring.consoleLogger("SSLSpec-client"),
                                          sslParams = None).unsafeRun

    try {
      val endpoint: Endpoint = Endpoint.single(transport)

      val fact = evaluate(endpoint, Monitoring.consoleLogger())(Client.factorial(10)).apply(Context.empty)

      an[Exception] should be thrownBy fact.map(_ ⇒ ()).unsafeRun
    } finally {
      shutdown.unsafeRun
      transport.shutdown.unsafeRun
    }
  }

  behavior of "SSL"
  it should "blah" in {
    val f = new java.io.File(getClass.getResource("/ssl-testing/CA.pem").getFile)
    f.exists should be (true)
  }

  it should "be able to parse a PEM" in {
    pems foreach { pemName =>
      val pemPath = Paths.get(getClass.getClassLoader.getResource(s"ssl-testing/$pemName").getPath)
      val x = SSL.certFromPEM(pemPath).runLog.unsafeAttemptRun
      x.isRight should be (true)
    }
  }

  it should "be able to parse a key" in {
    keys foreach { keyName =>
      val keyPath = Paths.get(getClass.getClassLoader.getResource(s"ssl-testing/$keyName").getPath)
      val x = SSL.keyFromPkcs8(keyPath).runLog.unsafeAttemptRun
      x.isRight should be (true)
    }
  }

  it should "be able to mutate a keystore" in {
    val caPEMPath = Paths.get(getClass.getClassLoader.getResource(s"ssl-testing/CA.pem").getPath)
    val clientPEMPath = Paths.get(getClass.getClassLoader.getResource(s"ssl-testing/client_cert.pem").getPath)
    val clientKeyPath = Paths.get(getClass.getClassLoader.getResource(s"ssl-testing/client_key.pk8").getPath)

    val keystore = SSL.emptyKeystore
    val x = (for {
               caMaybe <- SSL.certFromPEM(caPEMPath).last
               clMaybe <- SSL.certFromPEM(clientPEMPath).last
               keyMaybe <-SSL.keyFromPkcs8(clientKeyPath).last
               _ <- Stream.eval {
                 (for {
                   ca <- OptionT(Task.now(caMaybe))
                   cl <- OptionT(Task.now(clMaybe))
                   key <- OptionT(Task.now(keyMaybe))
                   _ <- OptionT.liftF(SSL.addCert(ca, "ca", keystore))
                   _ <- OptionT.liftF(SSL.addKey(key, List(ca, cl), "client", Array[Char](), keystore))
                 } yield ()).value
               }
             } yield()).run.unsafeAttemptRun

    x should be (Right(()))
  }

  it should "be able to generate an SSLContext" in {
    val caPEMPath = Paths.get(getClass.getClassLoader.getResource(s"ssl-testing/CA.pem").getPath)
    val clientPEMPath = Paths.get(getClass.getClassLoader.getResource(s"ssl-testing/client_cert.pem").getPath)
    val clientKeyPath = Paths.get(getClass.getClassLoader.getResource(s"ssl-testing/client_key.pk8").getPath)

    val keystore = SSL.emptyKeystore
    val keystoreTM = SSL.emptyKeystore
    val x = (for {
               caMaybe <- SSL.certFromPEM(caPEMPath).last
               clMaybe <- SSL.certFromPEM(clientPEMPath).last
               keyMaybe <- SSL.keyFromPkcs8(clientKeyPath).last
               _ <- Stream.eval {
                 (for {
                   ca <- OptionT(Task.now(caMaybe))
                   cl <- OptionT(Task.now(clMaybe))
                   key <- OptionT(Task.now(keyMaybe))
                   _ <- OptionT.liftF(SSL.addCert(ca, "ca", keystoreTM))
                   _ <- OptionT.liftF(SSL.addKey(key, List(ca, cl), "client", "changeit".toCharArray, keystore))
                 } yield ()).value
               }
             } yield {
               keystore -> keystoreTM
             }).run.unsafeAttemptRun

    x should be (Right(()))
  }
}

