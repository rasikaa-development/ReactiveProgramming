package com.ascend.reactive;

import reactor.core.publisher.Mono;

/**
 * Exercise 2: subscribing and understanding signals (onNext, onError, onComplete).
 *
 * <p>Every Reactive Streams sequence follows the grammar
 * {@code onSubscribe onNext* (onComplete | onError)?}: zero or more values, then at most ONE
 * terminal signal. onComplete and onError are mutually exclusive.
 */
public final class Exercise2Signals {

    private Exercise2Signals() {
    }

    public static void main(String[] args) {
        run();
    }

    public static void run() {
        Mono<String> first = Mono.just("Employee found");
        Mono<String> second = Mono.empty();
        Mono<String> third =
                Mono.error(new RuntimeException("Employee not found"));

        subscribeWithAllCallbacks("first  = Mono.just(\"Employee found\")", first);   // onNext + onComplete
        subscribeWithAllCallbacks("second = Mono.empty()", second);                  // onComplete only
        subscribeWithAllCallbacks("third  = Mono.error(...)", third);                // onError only

        // Supports question 3: no work happens until subscribe() is called.
        System.out.println("--- Laziness check: does anything run before subscribe()? ---");
        Mono<String> lookup = Mono.fromCallable(() -> {
            System.out.println("  [lookup] searching the employee records...");
            return "Employee found";
        });
        System.out.println("lookup Mono created - the search has NOT run yet");
        lookup.subscribe(value -> System.out.println("  onNext     -> value received: " + value));
    }

    /** Subscribes with three callbacks (value, error, completion), each printing a different message. */
    private static void subscribeWithAllCallbacks(String label, Mono<String> publisher) {
        System.out.println("--- Subscribing to " + label + " ---");
        publisher.subscribe(
                value -> System.out.println("  onNext     -> value received: " + value),
                error -> System.out.println("  onError    -> failed with: " + error.getMessage()),
                () -> System.out.println("  onComplete -> completed successfully"));
    }
}
