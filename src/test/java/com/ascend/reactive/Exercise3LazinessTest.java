package com.ascend.reactive;

import static org.junit.jupiter.api.Assertions.assertLinesMatch;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class Exercise3LazinessTest {

    @Test
    @DisplayName("Nothing is processed at creation; every subscription re-runs the pipeline; cache() runs it once")
    void printsExpectedOrder() {
        List<String> output = ConsoleCapture.linesPrintedBy(Exercise3Laziness::run);

        assertLinesMatch(List.of(
                "Pipeline created",
                "--- First subscription ---",
                "Processing: Arun",
                "Subscriber 1 received: Arun",
                "Processing: Priya",
                "Subscriber 1 received: Priya",
                "Processing: Kumar",
                "Subscriber 1 received: Kumar",
                "--- Second subscription ---",
                "Processing: Arun",
                "Subscriber 2 received: Arun",
                "Processing: Priya",
                "Subscriber 2 received: Priya",
                "Processing: Kumar",
                "Subscriber 2 received: Kumar",
                "--- Bonus: cache() shares a single execution ---",
                "Processing: Arun",
                "Cached subscriber 1 received: Arun",
                "Processing: Priya",
                "Cached subscriber 1 received: Priya",
                "Processing: Kumar",
                "Cached subscriber 1 received: Kumar",
                "Cached subscriber 2 received: Arun",
                "Cached subscriber 2 received: Priya",
                "Cached subscriber 2 received: Kumar"),
                output);
    }
}
