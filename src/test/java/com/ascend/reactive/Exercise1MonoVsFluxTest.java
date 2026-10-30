package com.ascend.reactive;

import static org.junit.jupiter.api.Assertions.assertLinesMatch;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class Exercise1MonoVsFluxTest {

    @Test
    @DisplayName("Mono emits 1 employee, Flux emits 3, empty publishers only complete")
    void printsExpectedSignals() {
        List<String> output = ConsoleCapture.linesPrintedBy(Exercise1MonoVsFlux::run);

        assertLinesMatch(List.of(
                "--- Task 3: subscribing to Mono<Employee> ---",
                "Mono emitted : Employee[id=101, name=Arun, department=CSE, salary=65000]",
                "Mono completed",
                "--- Task 3: subscribing to Flux<Employee> ---",
                "Flux emitted : Employee[id=101, name=Arun, department=CSE, salary=65000]",
                "Flux emitted : Employee[id=102, name=Priya, department=ECE, salary=55000]",
                "Flux emitted : Employee[id=103, name=Kumar, department=CSE, salary=75000]",
                "Flux completed",
                "--- Task 4a: Mono.empty() and Flux.empty() with a value consumer only ---",
                "(nothing printed above: the value consumer is never called)",
                "--- Task 4b: same publishers, also observing subscription and completion ---",
                "Empty Mono subscribed",
                "Empty Mono completed without emitting any value",
                "Empty Flux subscribed",
                "Empty Flux completed without emitting any value"),
                output);
    }
}
