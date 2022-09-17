/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import akka.stream.testkit.StreamSpec
import akka.stream.testkit.scaladsl.TestSink

import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.Future

class FlowStatefulMapAsyncSpec extends StreamSpec {

  "optimized version" in {
    // this version is deterministic bcs element is evaluated immediately without async boundary
    // it's work like synchronous, so state are updated every time f be evaluated

    implicit val ec = system.dispatcher

    val statefulMapSource = Source(Range(1, 3)).statefulMapAsync[Int, String](2)(
      () =>
        Future {
          Thread.sleep(ThreadLocalRandom.current().nextInt(1, 10))
          1
        },
      (s, e) => {
        Future.successful(s -> s"$e element")
      },
      s => {
        Future.successful(Some(s"$s state"))
      },
      (x, y) => x + y)

    println(statefulMapSource.toString)

    val mapSource = Source(Range(1, 3)).mapAsync(2)(x => Future.successful(s"$x element"))

    println(mapSource.toString)

    val probe = statefulMapSource.runWith(TestSink())

    val sub = probe.expectSubscription()
    sub.request(1)
    probe.expectNext("1 element")
    sub.request(1)
    probe.expectNext("2 element")
    sub.request(1)
    probe.expectNext("4 state")
    probe.expectComplete()
  }

  "unoptimized version" in {
    // this version is nondeterministic bcs f will be executed parallel
    // the state's updating may or may not be presented when f be called
    // if we change parallelism to 1, we got deterministic sequential behavior
    // if we remove the optimization, we got nondeterministic but consistent(always work in async boundary) behavior
    implicit val ec = system.dispatcher

    val probe =
      Source(Range(1, 3))
        .statefulMapAsync[Int, String](2)(
          () =>
            Future {
              1
            },
          (s, e) => {
            Future {
              s -> s"$e element"
            }
          },
          s => {
            Future.successful(Some(s"$s state"))
          },
          (x, y) => x + y)
        .runWith(TestSink())

    val sub = probe.expectSubscription()
    sub.request(1)
    probe.expectNext("1 element")
    sub.request(1)
    probe.expectNext("2 element")
    sub.request(1)
    probe.expectNext("4 state")
    probe.expectComplete()
  }

}
