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

import fs2.{ Strategy, Task }
import java.net.InetSocketAddress
import java.util.concurrent.Executors
import org.scalatest.{FlatSpec,Matchers,BeforeAndAfterAll}
import org.scalatest.matchers.{Matcher,MatchResult}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalacheck._
import Prop._
import transport.netty._

import natural.eq._

class RemoteSpec extends FlatSpec with Matchers with BeforeAndAfterAll with GeneratorDrivenPropertyChecks {
  import codecs._
  import Remote.implicits._

  private val env = Environment.empty
    .codec[Int]
    .codec[Double]
    .codec[List[Int]]
    .codec[List[Double]]
    .codec[List[Signature]]
    .populate { _
                 .declare("sum", (d: List[Int]) => Response.now(d.sum))
                 .declare("add3", (a: Int, b: Int, c: Int) => Response.now(a + b + c))
                 .declare("describe", Response.now(List(Signature("sum", List(Field("xs", "List[Double]")), "Double"),
                                                        Signature("sum", List(Field("xs", "List[Int]")), "Int"),
                                                        Signature("add1", List(Field("xs", "List[Int]")), "List[Int]"),
                                                        Signature("describe", Nil, "List[Signature]"))))

      .declare("sum", (d: List[Double]) => Response.now(d.sum))
      .declare("add1", (d: List[Int]) => Response.now(d.map(_ + 1):List[Int]))
    }

  private implicit val S: Strategy = Strategy.fromExecutor(fixedNamedThreadPool("test-strategy"))
  private val addr = new InetSocketAddress("localhost", 8082)
  private val server = env.serve(addr).unsafeRun
  private val nettyTrans = NettyTransport.single(addr).unsafeRun
  private val loc: Endpoint = Endpoint.single(nettyTrans)

  private val sum = Remote.ref[List[Int] => Int]("sum")
  private val sumD = Remote.ref[List[Double] => Double]("sum")
  private val mapI = Remote.ref[List[Int] => List[Int]]("add1")
  private val add3 = Remote.ref[(Int, Int, Int) => Int]("add3")

  private val ctx = Response.Context.empty

  override def afterAll() {
    server.unsafeRun
    nettyTrans.pool.close()
  }

  behavior of "roundtrip sum of List[Int]"
  it should "work" in {
    forAll { (l: List[Int], kvs: Map[String,String]) =>
      l.sum shouldBe sum(l).runWithoutContext(loc).unsafeRun
    }
  }

  behavior of "roundtrip sum of List[Double]"
  it should "work" in {
    forAll { (l: List[Double], kvs: Map[String,String]) =>
      l.sum shouldBe sumD(l).runWithContext(loc, ctx ++ kvs).unsafeRun
    }
  }

  behavior of "roundtrip map of List[Int]"
  it should "work" in {
    forAll { (l: List[Int], kvs: Map[String,String]) =>
      l.map(_ + 1) shouldBe mapI(l).runWithContext(loc, ctx ++ kvs).unsafeRun
    }
  }

  behavior of "check-serializers"
  it should "work" in {
    // verify that server returns a meaningful error when it asks for
    // decoder(s) the server does not know about
    val wrongsum = Remote.ref[List[Float] => Float]("sum")
    val t: Task[Float] = wrongsum(List(1.0f, 2.0f, 3.0f)).runWithContext(loc, ctx)
    t.unsafeAttemptRun.fold(
      e => {
        println("test resulted in error, as expected:")
        println(prettyError(e.toString))
        true
      },
      a => false
    ) shouldBe true
  }

  behavior of "check-declarations"
  it should "work" in {
    // verify that server returns a meaningful error when client asks
    // for a remote ref that is unknown
    val wrongsum = Remote.ref[List[Int] => Int]("product")
    val t: Task[Int] = wrongsum(List(1, 2, 3)).runWithContext(loc, ctx)
    t.unsafeAttemptRun.fold(
      e => {
        println("test resulted in error, as expected:")
        println(prettyError(e.toString))
        true
      },
      a => false
    ) shouldBe true
  }

  behavior of "roundtrip map of add3 Ints"
  it should "work" in {
    forAll { (one: Int, two: Int, three: Int) =>
      add3(one, two, three).runWithoutContext(loc).unsafeRun shouldBe (one + two + three)
    }
  }

  private def prettyError(msg: String) = msg.take(msg.indexOfSlice("stack trace:"))
}
