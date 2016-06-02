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

import cats.Monoid
import cats.implicits._

import fs2.{ io => fs2io, _ }
import fs2.interop.cats._
import fs2.util._

import java.io.{ByteArrayInputStream,File}
import java.nio.file.{Path,Paths}
import java.security.{KeyFactory,KeyStore,PrivateKey,SecureRandom}
import java.security.spec.{PKCS8EncodedKeySpec}
import java.security.cert.{Certificate,CertificateFactory,PKIXBuilderParameters,X509Certificate,X509CertSelector,TrustAnchor}
import javax.net.ssl._

import io.netty.handler.ssl._

import scala.collection.JavaConverters._

import scodec.bits.ByteVector

// what are all the configurations we want to support:
//
//  1. everything plaintext
//  2. traditional ssl
//  3. ssl + client cert

object SSL {

  def emptyKeystore: KeyStore = {
    val keystore = KeyStore.getInstance("JKS", "SUN")
    //Before a keystore can be accessed, it must be loaded.
    //Since we don't read keys from any file, we pass "null" and load certificate later in the code below
    keystore.load(null)
    keystore
  }

  def addPEM(path: Path, name: String)(ks: KeyStore): Task[KeyStore] = addCerts(ks, name, certFromPEM(path))

  implicit val taskUnitMonoid: Monoid[Task[Unit]] = new Monoid[Task[Unit]] {
    def empty = Task.now(())
    def combine(a: Task[Unit], b: Task[Unit]) = a flatMap { _ => b }
  }

  private[remotely] def addCert(cert: Certificate, name: String, ks: KeyStore): Task[Unit] = Task.delay(ks.setCertificateEntry(name, cert))
  private[remotely] def addKey(key: PrivateKey, certs: Seq[Certificate], name: String, pass: Array[Char], ks: KeyStore):Task[Unit] =
    Task.delay(ks.setKeyEntry(name, key, pass, certs.toArray[Certificate]))

  private[remotely] def addCerts(ks: KeyStore, name: String, certs: Stream[Task,Certificate])(implicit M: Monoid[Task[Unit]]): Task[KeyStore] =
    certs.through(pipe.zipWithIndex).runFold { M.empty } { case (t, (c, i)) => M.combine(t, addCert(c,name+i,ks)) }.map { _ => ks }

  private[remotely] def stripCruftFromPEM(withHeaders: Boolean): Pipe[Pure, String, String] = {
    def goBeginning: Stream.Handle[Pure, String] => Pull[Pure, String, Stream.Handle[Pure, String]] = Pull.receive1Option {
      case Some(str #: h) =>
        if (str startsWith "-----BEGIN") {
          if (withHeaders) Pull.output1(str + "\n") >> goEnd(h) else goEnd(h)
        } else goBeginning(h)
      case None => Pull.done
    }
    def goEnd: Stream.Handle[Pure, String] => Pull[Pure, String, Stream.Handle[Pure, String]] = Pull.receive1Option {
      case Some(str #: h) =>
        if(str startsWith "-----END") {
          if (withHeaders) Pull.output1(str + "\n") >> goBeginning(h) else goBeginning(h)
        } else Pull.output1(str + (if (withHeaders) "\n" else "")) >> goEnd(h)
      case None => Pull.fail(new IllegalArgumentException("Not a valid KEY, didn't find END marker"))
    }
    _ pull goBeginning
  }

  def pemString(path: Path, withHeaders: Boolean): Stream[Task,String] = fs2io.file.readAll[Task](path, 4096)
    .through(text.utf8Decode)
    .through(text.lines)
    .through(stripCruftFromPEM(withHeaders))
    .through(pipe.fold("")((o, i) => o ++ i))

  def certFromPEM(path: Path): Stream[Task,Certificate] =
    pemString(path, true) map { str =>
      CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(str.getBytes("US-ASCII")))
    }

  def keyFromPkcs8(path: Path): Stream[Task,PrivateKey] =
    pemString(path, false).flatMap { str =>
      ByteVector.fromBase64(str).fold[Stream[Task,PrivateKey]](Stream.fail(new IllegalArgumentException("could not parse PEM data"))){ decoded =>
        val spec = new PKCS8EncodedKeySpec(decoded.toArray)
        Stream.emit(KeyFactory.getInstance("RSA").generatePrivate(spec))
      }
    }

  private[remotely] def keyFromEncryptedPkcs8(path: Path, pass: Array[Char]): Stream[Task,PrivateKey] =
    pemString(path, false) map (str => KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(str.getBytes("US-ASCII"))))
}

case class SslParameters(caBundle: Option[File],
                         certFile: Option[File],
                         keyFile: Option[File],
                         keyPassword: Option[String],
                         enabledCiphers: Option[List[String]],
                         enabledProtocols: Option[List[String]],
                         requireClientAuth: Boolean) {
}

object SslParameters {

  private[remotely] def trustManagerForBundle(caBundle: File): Task[TrustManagerFactory] = for {
    trustSet <- Task.now(new java.util.HashSet[TrustAnchor]())
    _ <- SSL.certFromPEM(Paths.get(caBundle.getPath)).flatMap {
      case cert: X509Certificate =>
        val name = cert.getSubjectDN.getName
        if (cert.getBasicConstraints != -1) trustSet.add(new TrustAnchor(cert, null))
        Stream.eval(SSL.addCert(cert, name, SSL.emptyKeystore))
      case cert =>
        Stream.fail(new IllegalArgumentException("unexpected cert which is not x509"))
    }.run
    tm <- Task.delay {
      val tm = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
      val builder = new PKIXBuilderParameters(trustSet, new X509CertSelector)
      builder.setRevocationEnabled(false)
      val cptmp: CertPathTrustManagerParameters = new CertPathTrustManagerParameters(builder)
      tm.init(cptmp)
      tm
    }
  } yield tm

  private[remotely] def toClientContext(params: Option[SslParameters]): Task[Option[SslContext]] = {
    params.traverse { params =>
      for {
        tm <- params.caBundle.fold[Task[TrustManagerFactory]](Task.now(null))(trustManagerForBundle)
      } yield SslContextBuilder.forClient().
        sslProvider(SslProvider.JDK).
        trustManager(tm).
        keyManager(params.certFile.orNull, params.keyFile.orNull, params.keyPassword.orNull).
        ciphers(params.enabledCiphers.map(_.asJava).orNull, IdentityCipherSuiteFilter.INSTANCE).
        applicationProtocolConfig(ApplicationProtocolConfig.DISABLED).
        build()
    }
  }

  private[remotely] def toServerContext(params: Option[SslParameters]): Task[Option[SslContext]] = {
    params.traverse { params =>
      for {
        tm <- params.caBundle.fold[Task[TrustManagerFactory]](Task.now(null))(trustManagerForBundle)
      } yield SslContextBuilder.
        forServer(params.certFile.orNull, params.keyFile.orNull, params.keyPassword.orNull).
        sslProvider(SslProvider.JDK).
        trustManager(tm).
        ciphers(params.enabledCiphers.map(_.asJava).orNull, IdentityCipherSuiteFilter.INSTANCE).
        applicationProtocolConfig(ApplicationProtocolConfig.DISABLED).
        build()
    }
  }
}
