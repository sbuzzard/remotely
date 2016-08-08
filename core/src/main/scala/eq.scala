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

import cats.kernel.Eq

/** Utilities for working with `cats.kernel.Eq`. */
object natural {
  /**
   * Provides an `Eq` for all `A` where `A` is not "too generic".
   *
   * Ussage: `import natural.eq._`
   */
  object eq {

    /** Evidence trait that indicates type `A` is not too generic to have implicit natural equality defined. */
    trait NotTooGeneric[A]

    /**
     * Implicitly defines an `Eq` instance based on natrual equality as long as the caller can provide
     * implicit evidence that the inferred type A is not too generic.
     */
    implicit final def naturalEqual[A: NotTooGeneric]: Eq[A] = Eq.fromUniversalEquals[A]

    // First, define ambiguous (two) implicit instances for each type that's considered too generic

    implicit final def anyIsTooGeneric: NotTooGeneric[Any] = ???
    implicit final def anyIsTooGeneric2: NotTooGeneric[Any] = ???

    implicit final def anyRefIsTooGeneric: NotTooGeneric[AnyRef] = ???
    implicit final def anyRefIsTooGeneric2: NotTooGeneric[AnyRef] = ???

    implicit final def anyValIsTooGeneric: NotTooGeneric[AnyVal] = ???
    implicit final def anyValIsTooGeneric2: NotTooGeneric[AnyVal] = ???

    implicit final def productIsTooGeneric: NotTooGeneric[Product] = ???
    implicit final def productIsTooGeneric2: NotTooGeneric[Product] = ???

    // Now, for all types that aren't too generic (i.e., don't have ambiguous implicit NotTooGeneric instances available):

    /** Implicitly provides evidence for all types that they are not too generic. */
    implicit final def notTooGeneric[A]: NotTooGeneric[A] = new NotTooGeneric[A] {}
  }
}
