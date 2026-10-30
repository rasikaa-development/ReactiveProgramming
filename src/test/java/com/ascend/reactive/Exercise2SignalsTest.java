package com.ascend.reactive;

import static org.junit.jupiter.api.Assertions.assertLinesMatch;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class Exercise2SignalsTest {

    @Test
    @DisplayName("just -> onNext + onComplete, empty -> onComplete, error -> onError; nothing runs before subscribe")
    void printsExpectedCallbacks() {
        List<String> output = ConsoleCapture.linesPrintedBy(Exercise2Signals::run);

        assertLinesMatch(List.of(
                "--- Subscribing to first  = Mono.just(\"Employee found\") ---",
                "  onNext     -> value received: Employee found",
                "  onComplete -> completed successfully",
                "--- Subscribing to second = Mono.empty() ---",
                "  onComplete -> completed successfully",
                "--- Subscribing to third  = Mono.error(...) ---",
                "  onError    -> failed with: Employee not found",
                "--- Laziness check: does anything run before subscribe()? ---",
                "lookup Mono created - the search has NOT run yet",
                "  [lookup] searching the employee records...",
                "  onNext     -> value received: Employee found"),
                output);
    }
}
