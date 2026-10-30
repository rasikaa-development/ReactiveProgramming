package com.ascend.reactive;

import reactor.core.publisher.Flux;

/**
 * Exercise 3: laziness and re-subscription.
 *
 * <p>Building a pipeline only <em>describes</em> the work (assembly time). The work itself runs
 * when, and every time, a subscriber subscribes (subscription time).
 */
public final class Exercise3Laziness {

    private Exercise3Laziness() {
    }

    public static void main(String[] args) {
        run();
    }

    public static void run() {
        Flux<String> pipeline = Flux.just("Arun", "Priya", "Kumar")
                .doOnNext(name ->
                        System.out.println("Processing: " + name));

        System.out.println("Pipeline created");

        // TODO (completed): subscribe to the pipeline twice
        System.out.println("--- First subscription ---");
        pipeline.subscribe(name -> System.out.println("Subscriber 1 received: " + name));

        System.out.println("--- Second subscription ---");
        pipeline.subscribe(name -> System.out.println("Subscriber 2 received: " + name));

        // Bonus (supports answer 3): cache() runs the cold pipeline once and replays the results,
        // so the second subscriber does NOT trigger the "Processing:" side effects again.
        System.out.println("--- Bonus: cache() shares a single execution ---");
        Flux<String> cached = pipeline.cache();
        cached.subscribe(name -> System.out.println("Cached subscriber 1 received: " + name));
        cached.subscribe(name -> System.out.println("Cached subscriber 2 received: " + name));
    }
}
