package com.ascend.reactive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

class Exercise4OperatorsTest {

    @Test
    @DisplayName("filter -> doOnNext -> map -> take(2) emits ARUN, KUMAR and doOnNext sees only those two")
    void takeTwoPipeline() {
        List<String> doOnNextOutput = ConsoleCapture.linesPrintedBy(() ->
                StepVerifier.create(Exercise4Operators.firstTwoHighEarnerNames())
                        .expectNext("ARUN", "KUMAR")
                        .verifyComplete());

        assertEquals(List.of("Passed filter: Arun", "Passed filter: Kumar"), doOnNextOutput);
    }

    @Test
    @DisplayName("Extension: collectList() emits ONE list with every qualifying name")
    void collectListPipeline() {
        List<String> doOnNextOutput = ConsoleCapture.linesPrintedBy(() ->
                StepVerifier.create(Exercise4Operators.allHighEarnerNames())
                        .expectNext(List.of("ARUN", "KUMAR", "DIVYA", "RAVI"))
                        .verifyComplete());

        assertEquals(List.of(
                "Passed filter: Arun",
                "Passed filter: Kumar",
                "Passed filter: Divya",
                "Passed filter: Ravi"), doOnNextOutput);
    }

    @Test
    @DisplayName("Bonus: a prefetching operator before take(2) lets doOnNext see more names than the subscriber")
    void prefetchBeforeTake() {
        List<String> output = ConsoleCapture.linesPrintedBy(Exercise4Operators::prefetchBeforeTakeDemo);

        assertLinesMatch(List.of(
                "Passed filter: Arun",
                "Passed filter: Kumar",
                "Passed filter: Divya",
                "Passed filter: Ravi",
                "(publishOn hand-off to the subscriber runs now)",
                "Subscriber received: ARUN",
                "Subscriber received: KUMAR"),
                output);
    }
}
