# Reactive Programming – Assignment 1

Project Reactor exercises: Mono vs. Flux, signals, laziness, first operators, and moving from Flux to Mono.

- **Solutions, real console output and answers to every "Questions to consider":** [ANSWERS.md](ANSWERS.md)
- **Stack:** Java 17+ (records), Project Reactor `reactor-core` 3.7.18 (BOM `2024.0.17`), JUnit 5 + `reactor-test`

## Prerequisites

- JDK 17 or newer
- Maven 3.9+

## Run

```bash
./run.sh            # all five exercises
./run.sh 3          # only Exercise 3
./run.sh 1 4        # Exercises 1 and 4
./run.sh test       # unit tests (18)
```

Without the script:

```bash
mvn -q compile exec:java                                   # all exercises
mvn -q compile exec:java -Dexec.args="3"                   # one exercise
mvn -q compile exec:java -Dexec.mainClass=com.ascend.reactive.Exercise4Operators
mvn test
```

In an IDE, import the folder as a Maven project and run the `main` method of `MainApp` or of any `ExerciseN…` class.

> On JDK 24+, Maven 3.9 itself prints `WARNING: A terminally deprecated method in sun.misc.Unsafe has been called ...` at start-up. That warning comes from Maven's bundled Guice, not from this project, and is harmless.

## Structure

```
ReactiveProgramming/
├── pom.xml
├── run.sh
├── README.md
├── ANSWERS.md                         solutions + answers to all questions
└── src
    ├── main/java/com/ascend/reactive
    │   ├── MainApp.java               runs all (or selected) exercises
    │   ├── Exercise1MonoVsFlux.java   Mono.just / Flux.just / Mono.empty / Flux.empty
    │   ├── Exercise2Signals.java      onNext / onError / onComplete callbacks, laziness check
    │   ├── Exercise3Laziness.java     assembly vs. subscription, re-subscription, cache()
    │   ├── Exercise4Operators.java    filter, doOnNext, map, take(2), collectList, prefetch demo
    │   ├── Exercise5FluxToMono.java   Flux<Employee> -> Flux<String> -> Mono<List<String>>
    │   └── model/Employee.java        record Employee(int id, String name, String department, int salary)
    └── test/java/com/ascend/reactive
        ├── Exercise{1..5}*Test.java   assert each exercise's exact console output / emitted values
        ├── ReactorConceptsTest.java   StepVerifier tests backing the claims in ANSWERS.md
        ├── MainAppTest.java
        └── ConsoleCapture.java        captures System.out in tests
```
