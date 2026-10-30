package com.ascend.reactive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MainAppTest {

    @Test
    @DisplayName("Runs only the exercise whose number is passed")
    void runsSelectedExercise() {
        List<String> output = ConsoleCapture.linesPrintedBy(() -> MainApp.main(new String[] {"3"}));

        assertTrue(output.contains(" Exercise 3: Laziness and re-subscription"));
        assertFalse(output.stream().anyMatch(line -> line.startsWith(" Exercise 1:")));
    }

    @Test
    @DisplayName("Rejects an unknown exercise number")
    void rejectsUnknownExercise() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> MainApp.main(new String[] {"9"}));

        assertEquals("Unknown exercise '9'. Valid values: 1 to 5", error.getMessage());
    }
}
