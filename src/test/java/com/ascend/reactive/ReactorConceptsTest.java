package com.ascend.reactive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.concurrent.Queues;

/** Learning tests that back up the claims made in ANSWERS.md. */
@DisplayName("Reactor behaviour behind the written answers")
class ReactorConceptsTest {

    @Test
    @DisplayName("Ex1: a Mono holds at most one item - forcing two items into one fails")
    void monoHoldsAtMostOneItem() {
        StepVerifier.create(Flux.just("Arun", "Priya").single())
                .expectError(IndexOutOfBoundsException.class)
                .verify();
    }

    @Test
    @DisplayName("Ex1: an empty Mono has no value - block() returns null, defaultIfEmpty() supplies one")
    void emptyMonoHasNoValue() {
        assertNull(Mono.<String>empty().block());

        StepVerifier.create(Mono.<String>empty().defaultIfEmpty("No employee"))
                .expectNext("No employee")
                .verifyComplete();
    }

    @Test
    @DisplayName("Ex2: Mono.just() evaluates its argument eagerly, Mono.fromCallable() waits for subscribe()")
    void justIsEagerFromCallableIsLazy() {
        AtomicInteger evaluations = new AtomicInteger();

        Mono<Integer> eager = Mono.just(evaluations.incrementAndGet());
        assertEquals(1, evaluations.get(), "Mono.just() argument is evaluated when the line runs");

        Mono<Integer> lazy = Mono.fromCallable(evaluations::incrementAndGet);
        assertEquals(1, evaluations.get(), "fromCallable() has not run yet");

        StepVerifier.create(lazy).expectNext(2).verifyComplete();
        StepVerifier.create(eager).expectNext(1).verifyComplete();
    }

    @Test
    @DisplayName("Ex4: take(n) caps the upstream request at n; take(n, false) requests an unbounded amount")
    void takeCapsUpstreamDemand() {
        List<Long> requests = new CopyOnWriteArrayList<>();
        StepVerifier.create(Flux.range(1, 10).doOnRequest(requests::add).take(2))
                .expectNext(1, 2)
                .verifyComplete();
        assertEquals(List.of(2L), requests);

        requests.clear();
        StepVerifier.create(Flux.range(1, 10).doOnRequest(requests::add).take(2, false))
                .expectNext(1, 2)
                .verifyComplete();
        assertEquals(List.of(Long.MAX_VALUE), requests);
    }

    @Test
    @DisplayName("Ex4 bonus: publishOn() prefetches 256 items no matter how few the downstream wants")
    void publishOnPrefetches() {
        List<Long> requests = new CopyOnWriteArrayList<>();
        StepVerifier.create(Flux.range(1, 1_000)
                        .doOnRequest(requests::add)
                        .publishOn(Schedulers.immediate())
                        .take(2))
                .expectNext(1, 2)
                .verifyComplete();
        assertEquals(List.of((long) Queues.SMALL_BUFFER_SIZE), requests);
    }

    @Test
    @DisplayName("Ex5: collectList() of an empty Flux still emits one value - an empty list")
    void collectListOfEmptyFlux() {
        StepVerifier.create(Flux.<String>empty().collectList())
                .expectNext(List.of())
                .verifyComplete();
    }

    @Test
    @DisplayName("Ex5: collectList() propagates an error instead of emitting a partial list")
    void collectListPropagatesError() {
        Flux<String> failing = Flux.just("ARUN", "KUMAR")
                .concatWith(Flux.error(new RuntimeException("Database down")));

        StepVerifier.create(failing.collectList())
                .expectErrorMessage("Database down")
                .verify();
    }

    @Test
    @DisplayName("Ex5: collectList() of an infinite Flux never emits")
    void collectListOfInfiniteFluxNeverEmits() {
        StepVerifier.withVirtualTime(() -> Flux.interval(Duration.ofSeconds(1)).collectList())
                .expectSubscription()
                .expectNoEvent(Duration.ofHours(1))
                .thenCancel()
                .verify();
    }

    @Test
    @DisplayName("Ex5: block() is rejected on Reactor's non-blocking threads")
    void blockIsRejectedOnNonBlockingThreads() {
        Mono<List<String>> nameList = Flux.just("ARUN", "KUMAR").collectList();

        StepVerifier.create(Mono.fromCallable(nameList::block).subscribeOn(Schedulers.parallel()))
                .expectError(IllegalStateException.class)
                .verify(Duration.ofSeconds(5));
    }
}
