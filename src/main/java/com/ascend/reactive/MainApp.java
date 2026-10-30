package com.ascend.reactive;

import java.util.ArrayList;
import java.util.List;

/**
 * Runs every exercise in order, or only the exercises whose numbers are passed as arguments.
 *
 * <pre>
 *   mvn -q compile exec:java                      # all exercises
 *   mvn -q compile exec:java -Dexec.args="3"      # only Exercise 3
 *   mvn -q compile exec:java -Dexec.args="1 4"    # Exercises 1 and 4
 * </pre>
 */
public final class MainApp {

    private record Exercise(int number, String title, Runnable body) {
    }

    private static final List<Exercise> EXERCISES = List.of(
            new Exercise(1, "Mono vs. Flux - Creating publishers", Exercise1MonoVsFlux::run),
            new Exercise(2, "Subscribing and understanding signals", Exercise2Signals::run),
            new Exercise(3, "Laziness and re-subscription", Exercise3Laziness::run),
            new Exercise(4, "First operators - Build a pipeline", Exercise4Operators::run),
            new Exercise(5, "Moving between types - From Flux to Mono", Exercise5FluxToMono::run));

    private MainApp() {
    }

    public static void main(String[] args) {
        List<Exercise> selected = args.length == 0 ? EXERCISES : select(args);
        selected.forEach(MainApp::runWithBanner);
    }

    private static List<Exercise> select(String[] args) {
        List<Exercise> selected = new ArrayList<>();
        for (String arg : args) {
            selected.add(EXERCISES.stream()
                    .filter(exercise -> String.valueOf(exercise.number()).equals(arg.trim()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Unknown exercise '" + arg + "'. Valid values: 1 to " + EXERCISES.size())));
        }
        return selected;
    }

    private static void runWithBanner(Exercise exercise) {
        String rule = "=".repeat(72);
        System.out.println();
        System.out.println(rule);
        System.out.println(" Exercise " + exercise.number() + ": " + exercise.title());
        System.out.println(rule);
        exercise.body().run();
    }
}
