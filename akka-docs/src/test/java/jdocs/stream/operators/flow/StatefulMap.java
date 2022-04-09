/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.flow;

import akka.actor.typed.ActorSystem;
import akka.japi.Pair;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class StatefulMap {
    static final ActorSystem<?> system = null;

    public void indexed() {
        //#indexed
        Source.from(Arrays.asList("A", "B", "C", "D"))
            .statefulMap(() -> 0L,
                (index, element) -> Pair.create(index + 1, Pair.create(index, element)),
                Optional::ofNullable)
            .runForeach(System.out::println, system);
        //prints
        //Pair(0,A)
        //Pair(1,B)
        //Pair(2,C)
        //Pair(3,D)
        //#indexed
    }

    public void bufferUntilChanged() {
        // #bufferUntilChanged
        Source.from(Arrays.asList("A", "B", "B", "C", "C", "C", "D"))
            .statefulMap(() -> (List<String>) new LinkedList<String>(),
                (list, element) -> {
                    if (list.isEmpty()) {
                        list.add(element);
                        return Pair.create(list, Collections.<String>emptyList());
                    } else if (list.get(0).equals(element)) {
                        list.add(element);
                        return Pair.create(list, Collections.<String>emptyList());
                    } else {
                        final List<String> newBuffer = new LinkedList<>();
                        newBuffer.add(element);
                        return Pair.create(newBuffer, list);
                    }
                }, Optional::ofNullable)
            .runForeach(System.out::println, system);
        //prints
        //[A]
        //[B, B]
        //[C, C, C]
        //[D]
        //#bufferUntilChanged
    }

    public void distinctUntilChanged() {
        //#distinctUntilChanged
        Source.from(Arrays.asList("A", "B", "B", "C", "C", "C", "D"))
            .statefulMap(Optional::<String>empty,
                (lastElement, element) -> {
                    if (lastElement.isPresent() && lastElement.get().equals(element)) {
                        return Pair.create(lastElement, Optional.<String>empty());
                    } else {
                        return Pair.create(Optional.of(element), Optional.of(element));
                    }
                }, Optional::ofNullable)
            .via(Flow.flattenOptional())
            .runForeach(System.out::println, system);
        //prints
        //A
        //B
        //C
        //D
        //#distinctUntilChanged
    }

    public void usingResource() {
        // #usingResource
        Source.from(Arrays.asList("A", "B", "C", "D"))
            .statefulMap(() -> {
                //allocates some kind of new resource
                final AtomicInteger resource = new AtomicInteger(0);
                resource.set(100);
                return resource;
            }, (resource, element) -> {
                //mock resource usage
                final String output = "element " + element + " use resource: " + resource.getAndDecrement();
                return Pair.create(resource, output);
            }, r -> {
                //release resource
                r.set(0);
                return Optional.empty();
            }).runForeach(System.out::println, system);
        //prints
        //element A use resource: 100
        //element B use resource: 99
        //element C use resource: 98
        //element D use resource: 97
        //#usingResource
    }
}
