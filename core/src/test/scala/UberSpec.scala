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

import fs2.{Strategy,Stream,Task}

import org.scalatest.matchers.{Matcher,MatchResult}
import org.scalatest.{FlatSpec,Matchers,BeforeAndAfterAll}
import remotely.transport.netty.NettyTransport
import scala.concurrent.duration.DurationInt
import java.util.concurrent.Executors

class UberSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit val S: Strategy = Strategy.fromExecutor(fixedNamedThreadPool("test-strategy"))
  behavior of "permutations"
  it should "work" in {
    val input: Stream[Task,Int] = Stream.emits(Vector(1,2,3)).covary[Task]
    val permuted : IndexedSeq[Stream[Task,Int]] = Endpoint.permutations(input).runLog.unsafeRun
    permuted.size should be (6)
    val all = permuted.map(_.runLog.unsafeRun).toSet
    all should be(Set(IndexedSeq(1,2,3), IndexedSeq(1,3,2), IndexedSeq(2,1,3), IndexedSeq(2,3,1), IndexedSeq(3,1,2), IndexedSeq(3,2,1)))
  }

  behavior of "isEmpty"
  it should "work" in  {
    val empty: Stream[Task,Int] = Stream.empty.covary[Task]
    Endpoint.isEmpty(empty).unsafeRun should be (true)

    val notEmpty: Stream[Task,Int] = Stream.emit(1).covary[Task]
    Endpoint.isEmpty(notEmpty).unsafeRun should be (false)

    val alsoNot:  Stream[Task,Int] = Stream.eval(Task.now(1))
    Endpoint.isEmpty(alsoNot).unsafeRun should be (false)
  }

  behavior of "transpose"
  it should "work" in {
    val input = IndexedSeq(IndexedSeq("a", "b", "c"),IndexedSeq("q", "w", "e"), IndexedSeq("1", "2", "3"))
    val inputStream: Stream[Task,Stream[Task,String]] = Stream.emits(input.map(Stream.emits(_).covary[Task])).covary[Task]
    val transposed: IndexedSeq[IndexedSeq[String]] = Endpoint.transpose(inputStream).runLog.unsafeRun.map(_.runLog.unsafeRun)
    transposed should be (input.transpose)
  }

  val addr1 = new java.net.InetSocketAddress("localhost", 9000)
  val addr2 = new java.net.InetSocketAddress("localhost", 9009)

  val server1 = new CountServer
  val server2 = new CountServer
  val shutdown1 = server1.environment.serve(addr1).unsafeRun
  val shutdown2 = server2.environment.serve(addr2).unsafeRun

  override def afterAll() {
    shutdown1.unsafeRun
    shutdown2.unsafeRun
  }

  val endpoint1 = (NettyTransport.single(addr1) map Endpoint.single).unsafeRun
  val endpoint2 = (NettyTransport.single(addr2) map Endpoint.single).unsafeRun
  def endpoints: Stream[Nothing,Endpoint] = Stream.emits(List(endpoint1, endpoint2)) ++ endpoints
  val endpointUber = Endpoint.uber(1 second, 10 seconds, 10, endpoints)

  behavior of "uber"
  ignore should "work" in { // this seems to hang
    import Response.Context
    import Remote.implicits._
    import codecs._
    val call = evaluate(endpointUber, Monitoring.empty)(CountClient.ping(1))

    val i: Int = call.apply(Context.empty).unsafeRun
    val j: Int = call.apply(Context.empty).unsafeRun
    j should be (2)
  }
}
