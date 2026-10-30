package com.ascend.reactive;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Test helper that records everything an action prints to {@code System.out}. */
final class ConsoleCapture {

    private ConsoleCapture() {
    }

    /** Runs {@code action} with {@code System.out} redirected and returns the printed lines. */
    static List<String> linesPrintedBy(Runnable action) {
        PrintStream original = System.out;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (PrintStream capture = new PrintStream(buffer, true, StandardCharsets.UTF_8)) {
            System.setOut(capture);
            action.run();
        } finally {
            System.setOut(original);
        }
        return buffer.toString(StandardCharsets.UTF_8).lines().toList();
    }
}
