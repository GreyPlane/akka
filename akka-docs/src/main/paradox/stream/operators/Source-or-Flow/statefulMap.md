# statefulMap

Transform each element into a state and an element that is individually passed downstream.

@ref[Simple operators](../index.md#simple-operators)

## Signature

@apidoc[Flow.statefulMap](Flow) { scala="#statefulMap%5BS%2CT%5D%28create%3A%28%29%3D%3ES%29%28f%3A%28S%2COut%29%20%3D%3E%28S%2CT%29%2ConComplete%3AS%3D%3EOption%5BT%5D%29%3ARepr%5BT%5D" java="#statefulMap(akka.japi.function.Creator,akka.japi.function.Function2,akka.japi.function.Function)" }

## Description

Transform each stream element with the help of a state. 

The state creation function is invoked once when the stream is materialized and the returned state is passed to the mapping function for mapping the first element. 

The mapping function returns a mapped element to emit downstream and a state to pass to the next mapping function. The state can be the same for each mapping return, be a new immutable state but it is also safe to use a mutable state.

The on complete function is called, once, when the first of upstream completion, downstream cancellation or stream failure happens. If the cause is upstream completion and the downstream is still accepting elements, the returned value from the function is passed downstream before completing the operator itself, for the other cases the returned value is ignored.

The `statefulMap` operator adheres to the
ActorAttributes.SupervisionStrategy attribute.

For mapping stream elements without keeping a state see @ref:[map](map.md).

## Examples

In the first example we will try to implement a simple `indexed` operator.

Scala
:  @@snip [StatefulMap.scala](/akka-docs/src/test/scala/docs/stream/operators/flow/StatefulMap.scala) { #indexed }

Java
:   @@snip [StatefulMap.java](/akka-docs/src/test/java/jdocs/stream/operators/flow/StatefulMap.java) { #indexed }

In this example we implemented an `indexed` operator,it always associate a unique index 
to each element of the stream, the index starts from 0.

In the second example we keep buffering until the element is changed.

Scala
:  @@snip [StatefulMap.scala](/akka-docs/src/test/scala/docs/stream/operators/flow/StatefulMap.scala) { #bufferUntilChanged }

Java
:   @@snip [StatefulMap.java](/akka-docs/src/test/java/jdocs/stream/operators/flow/StatefulMap.java) { #bufferUntilChanged }

In this example, the elements are keep buffering until the element is changed, and then the buffer is emitted downstream.
When upstream completed, we defer the completion and emit the ongoing buffer to downstream too.

In the third example we will distinguish between the continuing elements.

Scala
:  @@snip [StatefulMap.scala](/akka-docs/src/test/scala/docs/stream/operators/flow/StatefulMap.scala) { #distinctUntilChanged }

Java
:   @@snip [StatefulMap.java](/akka-docs/src/test/java/jdocs/stream/operators/flow/StatefulMap.java) { #distinctUntilChanged }

In this example, the elements duplicated with the previous one will be dropped.

In the forth example we will use some kind of resource when handling the element,
and do clean up when the stream is complete.

Scala
:  @@snip [StatefulMap.scala](/akka-docs/src/test/scala/docs/stream/operators/flow/StatefulMap.scala) { #usingResource }

Java
:   @@snip [StatefulMap.java](/akka-docs/src/test/java/jdocs/stream/operators/flow/StatefulMap.java) { #usingResource }

In this example, when the @apidoc[Flow.statefulMap](Flow) started, we acquire a resource, and keep using the resource
during the stream is running. When the stream is complete, we release.

When the stream completes unexpectedly, the return value of the `onComplete` function will be ignored.

## Reactive Streams semantics

@@@div { .callout }

**emits** the mapping function returns an element and downstream is ready to consume it

**backpressures** downstream backpressures

**completes** upstream completes

**cancels** downstream cancels

@@@
