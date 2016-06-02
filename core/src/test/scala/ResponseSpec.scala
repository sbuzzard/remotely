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

import cats.Monad
import cats.implicits._

import fs2.Strategy
import fs2.interop.cats._

import org.scalacheck._
import Prop._
import scala.concurrent.{ExecutionContext,Future}

object ResponseSpec extends Properties("Response") {

  property("stack safety") = {
    import ExecutionContext.Implicits.global
    implicit val S: Strategy = Strategy.fromExecutionContext(global)
    val N = 100000
    val responses = (0 until N).map(Monad[Response].pure(_))
    val responses2 = (0 until N).map(i => Response.async(Future(i)))

    def leftFold(responses: Seq[Response[Int]]): Response[Int] =
      responses.foldLeft(Monad[Response].pure(0))((hd,tl) => (hd |@| tl).map(_ + _))

    def rightFold(responses: Seq[Response[Int]]): Response[Int] =
      responses.reverse.foldLeft(Monad[Response].pure(0))((tl,hd) => (hd |@| tl).map(_ + _))

    val ctx = Response.Context.empty
    val expected = (0 until N).sum

    leftFold(responses)(ctx).unsafeRun == expected &&
    rightFold(responses)(ctx).unsafeRun == expected &&
    leftFold(responses2)(ctx).unsafeRun == expected &&
    rightFold(responses2)(ctx).unsafeRun == expected
  }
}
