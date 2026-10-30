package com.ascend.reactive;

import static org.junit.jupiter.api.Assertions.assertLinesMatch;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class Exercise5FluxToMonoTest {

    @Test
    @DisplayName("names emits each name separately; nameList emits one list; collectList() is not a List")
    void printsExpectedResults() {
        List<String> output = ConsoleCapture.linesPrintedBy(Exercise5FluxToMono::run);

        assertLinesMatch(List.of(
                "collectList() returned a MonoCollectList, not a java.util.List",
                "--- names (Flux<String>): one onNext per name ---",
                "names emitted    : ARUN",
                "names emitted    : KUMAR",
                "names completed",
                "--- nameList (Mono<List<String>>): ONE onNext carrying the whole list ---",
                "nameList emitted : [ARUN, KUMAR]",
                "nameList completed",
                "nameList.block() returned a plain ArrayList: [ARUN, KUMAR]"),
                output);
    }
}
