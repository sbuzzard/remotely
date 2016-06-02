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

import cats.data.Xor

import fs2.Task

import scodec.Attempt.{Successful, Failure}
import scodec.{Attempt, Err}

import remotely.codecs.DecodingFailure

package object utils {
  implicit class AugmentedXor[E,A](a: Xor[E, A]) {
    def toTask(implicit conv: E => Throwable): Task[A] = a match {
      case Xor.Left(e) => Task.fail(conv(e))
      case Xor.Right(a) => Task.now(a)
    }
  }
  implicit class AugmentedEither[E,A](a: Either[E, A]) {
    def toTask(implicit conv: E => Throwable): Task[A] = a match {
      case Left(e) => Task.fail(conv(e))
      case Right(a) => Task.now(a)
    }
  }
  implicit def errToE(err: Err) = new DecodingFailure(err)
}
