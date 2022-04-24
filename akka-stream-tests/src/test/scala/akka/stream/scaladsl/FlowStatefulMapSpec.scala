/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import akka.Done
import akka.stream.{ AbruptStageTerminationException, ActorAttributes, ActorMaterializer, Supervision }
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource

import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt
import scala.util.Success
import scala.util.control.NoStackTrace

class FlowStatefulMapSpec extends StreamSpec {

  val ex = new Exception("TEST") with NoStackTrace

  "A StatefulMap" must {
    "work in the happy case" in {
      val sinkProb = Source(List(1, 2, 3, 4, 5))
        .statefulMap(() => 0)((agg, elem) => {
          (agg + elem, (agg, elem))
        }, _ => None)
        .runWith(TestSink.probe[(Int, Int)])
      sinkProb.expectSubscription().request(6)
      sinkProb
        .expectNext((0, 1))
        .expectNext((1, 2))
        .expectNext((3, 3))
        .expectNext((6, 4))
        .expectNext((10, 5))
        .expectComplete()
    }

    "can remember the state when complete" in {
      val sinkProb = Source(List(1, 2, 3, 4, 5))
        .statefulMap(() => List.empty[Int])(
          (grouped, elem) => {
            //grouped 3 elements into a list
            val state = elem :: grouped
            if (state.size == 3)
              (Nil, state.reverse)
            else
              (state, Nil)
          },
          state => Some(state.reverse))
        .filterNot(_.isEmpty)
        .runWith(TestSink.probe[List[Int]])
      sinkProb.expectSubscription().request(2)
      sinkProb.expectNext(List(1, 2, 3)).expectNext(List(4, 5)).expectComplete()
    }
    def printThreadName(id: String) = println(s"${Thread.currentThread().getName}-$id")

    "optimized version" in {
      // this version is deterministic bcs element is evaluated immediately without async boundary
      // it's work like synchronous, so state are updated every time f be evaluated

      implicit val ec = system.dispatcher

      val probe = Source(Range(1, 3))
        .statefulMapAsync[Int, String](2)(() => Future { printThreadName("create"); 1 }, (s, e) => {
          Future.successful(s -> s"$e element")
        }, s => {
          Some(s"$s state")
        }, (x, y) => x + y)
        .runWith(TestSink.probe)

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
          .statefulMapAsync[Int, String](2)(() => Future { printThreadName("create"); 1 }, (s, e) => {
            Future { s -> s"$e element" }
          }, s => {
            Some(s"$s state")
          }, (x, y) => x + y)
          .runWith(TestSink.probe)

      val sub = probe.expectSubscription()
      sub.request(1)
      probe.expectNext("1 element")
      sub.request(1)
      probe.expectNext("2 element")
      sub.request(1)
      probe.expectNext("4 state")
      probe.expectComplete()
    }

    "be able to restart" in {
      val testSink = Source(List(1, 2, 3, 4, 5))
        .statefulMap(() => 0)((agg, elem) => {
          if (elem % 3 == 0)
            throw ex
          else
            (agg + elem, (agg, elem))
        }, _ => None)
        .withAttributes(ActorAttributes.supervisionStrategy(Supervision.restartingDecider))
        .runWith(TestSink.probe[(Int, Int)])

      testSink.expectSubscription().request(5)
      testSink.expectNext((0, 1)).expectNext((1, 2)).expectNext((0, 4)).expectNext((4, 5)).expectComplete()
    }

    "be able to stop" in {
      val testSink = Source(List(1, 2, 3, 4, 5))
        .statefulMap(() => 0)((agg, elem) => {
          if (elem % 3 == 0)
            throw ex
          else
            (agg + elem, (agg, elem))
        }, _ => None)
        .withAttributes(ActorAttributes.supervisionStrategy(Supervision.stoppingDecider))
        .runWith(TestSink.probe[(Int, Int)])

      testSink.expectSubscription().request(5)
      testSink.expectNext((0, 1)).expectNext((1, 2)).expectError(ex)
    }

    "fail on upstream failure" in {
      val (testSource, testSink) = TestSource
        .probe[Int]
        .statefulMap(() => 0)((agg, elem) => {
          (agg + elem, (agg, elem))
        }, _ => None)
        .toMat(TestSink.probe[(Int, Int)])(Keep.both)
        .run()

      testSink.expectSubscription().request(5)
      testSource.sendNext(1)
      testSink.expectNext((0, 1))
      testSource.sendNext(2)
      testSink.expectNext((1, 2))
      testSource.sendNext(3)
      testSink.expectNext((3, 3))
      testSource.sendNext(4)
      testSink.expectNext((6, 4))
      testSource.sendError(ex)
      testSink.expectError(ex)
    }

    "defer upstream failure and remember state" in {
      val (testSource, testSink) = TestSource
        .probe[Int]
        .statefulMap(() => 0)((agg, elem) => { (agg + elem, (agg, elem)) }, (state: Int) => Some((state, -1)))
        .toMat(TestSink.probe[(Int, Int)])(Keep.both)
        .run()

      testSink.expectSubscription().request(5)
      testSource.sendNext(1)
      testSink.expectNext((0, 1))
      testSource.sendNext(2)
      testSink.expectNext((1, 2))
      testSource.sendNext(3)
      testSink.expectNext((3, 3))
      testSource.sendNext(4)
      testSink.expectNext((6, 4))
      testSource.sendError(ex)
      testSink.expectNext((10, -1))
      testSink.expectError(ex)
    }

    "cancel upstream when downstream cancel" in {
      val promise = Promise[Done]()
      val testSource = TestSource
        .probe[Int]
        .statefulMap(() => 100)((agg, elem) => {
          (agg + elem, (agg, elem))
        }, (state: Int) => {
          promise.complete(Success(Done))
          Some((state, -1))
        })
        .toMat(Sink.cancelled)(Keep.left)
        .run()
      testSource.expectSubscription().expectCancellation()
      Await.result(promise.future, 3.seconds) shouldBe Done
    }

    "cancel upstream when downstream fail" in {
      val promise = Promise[Done]()
      val testProb = TestSubscriber.probe[(Int, Int)]()
      val testSource = TestSource
        .probe[Int]
        .statefulMap(() => 100)((agg, elem) => {
          (agg + elem, (agg, elem))
        }, (state: Int) => {
          promise.complete(Success(Done))
          Some((state, -1))
        })
        .toMat(Sink.fromSubscriber(testProb))(Keep.left)
        .run()
      testProb.cancel(ex)
      testSource.expectCancellationWithCause(ex)
      Await.result(promise.future, 3.seconds) shouldBe Done
    }

    "call its onComplete callback on abrupt materializer termination" in {
      @nowarn("msg=deprecated")
      val mat = ActorMaterializer()
      val promise = Promise[Done]()

      val matVal = Source
        .single(1)
        .statefulMap(() => -1)((_, elem) => (elem, elem), _ => {
          promise.complete(Success(Done))
          None
        })
        .runWith(Sink.never)(mat)
      mat.shutdown()
      matVal.failed.futureValue shouldBe a[AbruptStageTerminationException]
      Await.result(promise.future, 3.seconds) shouldBe Done
    }

    "call its onComplete callback when stop" in {
      val promise = Promise[Done]()
      Source
        .single(1)
        .statefulMap(() => -1)((_, elem) => {
//          throw ex
          (elem, elem)
        }, _ => {
          promise.complete(Success(Done))
          None
        })
        .runWith(Sink.ignore)
      Await.result(promise.future, 3.seconds) shouldBe Done
    }

    "be able to be used as indexed" in {
      Source(List("A", "B", "C", "D"))
        .statefulMap(() => 0L)((idx, elem) => (idx + 1, (idx, elem)), _ => None)
        .runWith(TestSink.probe[(Long, String)])
        .request(4)
        .expectNext((0L, "A"))
        .expectNext((1L, "B"))
        .expectNext((2L, "C"))
        .expectNext((3L, "D"))
        .expectComplete()
    }

    "be able to be used as bufferUntilChanged" in {
      val sink = TestSink.probe[List[String]]
      Source("A" :: "B" :: "B" :: "C" :: "C" :: "C" :: "D" :: Nil)
        .statefulMap(() => List.empty[String])(
          (buffer, elem) =>
            buffer match {
              case Nil                       => (elem :: Nil, Nil)
              case head :: _ if head != elem => (elem :: Nil, buffer.reverse)
              case _                         => (elem :: buffer, Nil)
            },
          buffer => Some(buffer.reverse))
        .filter(_.nonEmpty)
        .alsoTo(Sink.foreach(println))
        .runWith(sink)
        .request(4)
        .expectNext(List("A"))
        .expectNext(List("B", "B"))
        .expectNext(List("C", "C", "C"))
        .expectNext(List("D"))
        .expectComplete()
    }

    "be able to be used as distinctUntilChanged" in {
      Source("A" :: "B" :: "B" :: "C" :: "C" :: "C" :: "D" :: Nil)
        .statefulMap(() => Option.empty[String])(
          (lastElement, elem) =>
            lastElement match {
              case Some(head) if head == elem => (Some(elem), None)
              case _                          => (Some(elem), Some(elem))
            },
          _ => None)
        .collect({ case Some(elem) => elem })
        .runWith(TestSink.probe[String])
        .request(4)
        .expectNext("A")
        .expectNext("B")
        .expectNext("C")
        .expectNext("D")
        .expectComplete()
    }

    "be able to using resource" in {
      val resource = new AtomicInteger(0)
      def allocateResource: AtomicInteger = {
        resource.set(100)
        resource
      }
      Source(List("A", "B", "C", "D"))
        .statefulMap(() => allocateResource)((resource, elem) => {
          //use the resource
          (resource, s"element $elem use resource: " + resource.getAndDecrement())
        }, resource => {
          resource.set(0)
          None
        })
        .alsoTo(Sink.foreach(println))
        .runWith(TestSink.probe[String])
        .request(4)
        .expectNext("element A use resource: 100")
        .expectNext("element B use resource: 99")
        .expectNext("element C use resource: 98")
        .expectNext("element D use resource: 97")
        .expectComplete()
      resource.get() shouldBe 0
    }

    "be able to use resource per element" in {
      Source(List("A", "B", "C", "D"))
        .flatMapConcat { elem =>
          Source
            .single(elem)
            .statefulMap(() => new AtomicInteger(1))((r, elem) => {
              //use the resource
              (r, s"element $elem use resource: " + r.getAndDecrement())
            }, r => {
              r.set(0)
              None
            })
        }
        .runWith(TestSink.probe[String])
        .request(4)
        .expectNext("element A use resource: 1")
        .expectNext("element B use resource: 1")
        .expectNext("element C use resource: 1")
        .expectNext("element D use resource: 1")
        .expectComplete()
    }

    "be able to use async resource per element" in {
      implicit val ec = system.dispatcher
      Source(List("A", "B", "C", "D"))
        .flatMapConcat { elem =>
          Source
            .single(elem)
            .statefulMap(() => Future(new AtomicInteger(1)))(
              (r, elem) => {
                //use the resource
                val asyncCompute = r.map(c => s"element $elem use resource: " + c.getAndDecrement())
                (asyncCompute.flatMap(_ => r).recoverWith({ case _ => r }), asyncCompute)
              },
              r => {
                r.foreach(_.set(0))
                None
              })
        }
        .mapAsync(1)(identity)
        .runWith(TestSink.probe[String])
        .request(4)
        .expectNext("element A use resource: 1")
        .expectNext("element B use resource: 1")
        .expectNext("element C use resource: 1")
        .expectNext("element D use resource: 1")
        .expectComplete()
    }
  }
}
