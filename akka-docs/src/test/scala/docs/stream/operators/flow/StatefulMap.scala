/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.flow
import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

import java.util.concurrent.atomic.AtomicInteger

object StatefulMap {

  implicit val actorSystem: ActorSystem = ???

  def indexed(): Unit = {
    //#indexed
    Source(List("A", "B", "C", "D"))
      .statefulMap(() => 0L)((idx, elem) => (idx + 1, (idx, elem)), _ => None)
      .runWith(Sink.foreach(println))
    //prints
    //(0,A)
    //(1,B)
    //(2,C)
    //(3,D)
    //#indexed
  }

  def bufferUntilChanged(): Unit = {
    //#bufferUntilChanged
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
      .runWith(Sink.foreach(println))
    //prints
    //List(A)
    //List(B, B)
    //List(C, C, C)
    //List(D)
    //#bufferUntilChanged
  }

  def distinctUntilChanged(): Unit = {
    //#distinctUntilChanged
    Source("A" :: "B" :: "B" :: "C" :: "C" :: "C" :: "D" :: Nil)
      .statefulMap(() => Option.empty[String])(
        (lastElement, elem) =>
          lastElement match {
            case Some(head) if head == elem => (Some(elem), None)
            case _                          => (Some(elem), Some(elem))
          },
        _ => None)
      .collect { case Some(elem) => elem }
      .runWith(Sink.foreach(println))
    //prints
    //A
    //B
    //C
    //D
    //#distinctUntilChanged
  }

  def usingResource(): Unit = {
    //#usingResource
    def allocateResource: AtomicInteger = {
      //allocates some kind of new resource
      val resource = new AtomicInteger(0)
      resource.set(100)
      resource
    }
    Source(List("A", "B", "C", "D"))
      .statefulMap(() => allocateResource)(
        (resource, elem) => {
          //use the resource
          (resource, s"element $elem use resource: " + resource.getAndDecrement())
        },
        resource => {
          //release resource
          resource.set(0)
          None
        })
      .runWith(Sink.foreach(println))
    //prints
    //element A use resource: 100
    //element B use resource: 99
    //element C use resource: 98
    //element D use resource: 97
    //#usingResource
  }

}
