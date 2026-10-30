package com.ascend.reactive;

import com.ascend.reactive.model.Employee;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Exercise 1: Mono vs. Flux - creating publishers.
 *
 * <ul>
 *   <li>{@link Mono}: publishes <b>0 or 1</b> item, then onComplete (or onError).</li>
 *   <li>{@link Flux}: publishes <b>0 to N</b> items (N may be unbounded), then onComplete (or onError).</li>
 * </ul>
 */
public final class Exercise1MonoVsFlux {

    private Exercise1MonoVsFlux() {
    }

    public static void main(String[] args) {
        run();
    }

    public static void run() {
        // Task 1: a Mono that emits ONE employee.
        Mono<Employee> employeeMono = Mono.just(new Employee(101, "Arun", "CSE", 65000));

        // Task 2: a Flux that emits THREE employees.
        Flux<Employee> employeeFlux = Flux.just(
                new Employee(101, "Arun", "CSE", 65000),
                new Employee(102, "Priya", "ECE", 55000),
                new Employee(103, "Kumar", "CSE", 75000));

        // Task 3: subscribe to both publishers and print the emitted employee objects.
        System.out.println("--- Task 3: subscribing to Mono<Employee> ---");
        employeeMono.subscribe(
                employee -> System.out.println("Mono emitted : " + employee),
                error -> System.out.println("Mono error   : " + error.getMessage()),
                () -> System.out.println("Mono completed"));

        System.out.println("--- Task 3: subscribing to Flux<Employee> ---");
        employeeFlux.subscribe(
                employee -> System.out.println("Flux emitted : " + employee),
                error -> System.out.println("Flux error   : " + error.getMessage()),
                () -> System.out.println("Flux completed"));

        // Task 4: empty publishers.
        Mono<Employee> emptyMono = Mono.empty();
        Flux<Employee> emptyFlux = Flux.empty();

        System.out.println("--- Task 4a: Mono.empty() and Flux.empty() with a value consumer only ---");
        emptyMono.subscribe(employee -> System.out.println("Empty Mono emitted: " + employee));
        emptyFlux.subscribe(employee -> System.out.println("Empty Flux emitted: " + employee));
        System.out.println("(nothing printed above: the value consumer is never called)");

        System.out.println("--- Task 4b: same publishers, also observing subscription and completion ---");
        emptyMono
                .doOnSubscribe(subscription -> System.out.println("Empty Mono subscribed"))
                .subscribe(
                        employee -> System.out.println("Empty Mono emitted: " + employee),
                        error -> System.out.println("Empty Mono error  : " + error.getMessage()),
                        () -> System.out.println("Empty Mono completed without emitting any value"));
        emptyFlux
                .doOnSubscribe(subscription -> System.out.println("Empty Flux subscribed"))
                .subscribe(
                        employee -> System.out.println("Empty Flux emitted: " + employee),
                        error -> System.out.println("Empty Flux error  : " + error.getMessage()),
                        () -> System.out.println("Empty Flux completed without emitting any value"));
    }
}
