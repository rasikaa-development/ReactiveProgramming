package com.ascend.reactive;

import com.ascend.reactive.model.Employee;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Locale;
import java.util.Queue;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Exercise 4: first operators - filter(), doOnNext(), map(), take() and collectList().
 */
public final class Exercise4Operators {

    static final int MIN_SALARY = 60_000;

    private Exercise4Operators() {
    }

    public static void main(String[] args) {
        run();
    }

    public static void run() {
        System.out.println("--- Tasks 1-5: filter -> doOnNext -> map -> take(2) ---");
        firstTwoHighEarnerNames()
                .subscribe(name -> System.out.println("Subscriber received: " + name));      // Task 5

        System.out.println("--- Extension: take(2) removed, collectList() added ---");
        allHighEarnerNames()
                .subscribe(names -> System.out.println("Collected list: " + names));

        System.out.println("--- Bonus: a case where doOnNext() prints MORE names than the subscriber gets ---");
        prefetchBeforeTakeDemo();
    }

    static Flux<Employee> employees() {
        return Flux.just(
                new Employee(101, "Arun", "CSE", 65000),
                new Employee(102, "Priya", "ECE", 55000),
                new Employee(103, "Kumar", "CSE", 75000),
                new Employee(104, "Divya", "IT", 80000),
                new Employee(105, "Ravi", "ECE", 72000));
    }

    /** Tasks 1-4. The element type changes from Employee to String at map(). */
    static Flux<String> firstTwoHighEarnerNames() {
        return employees()
                .filter(employee -> employee.salary() >= MIN_SALARY)                           // 1. keep salary >= 60,000
                .doOnNext(employee -> System.out.println("Passed filter: " + employee.name())) // 2. side effect only
                .map(employee -> employee.name().toUpperCase(Locale.ROOT))                     // 3. Employee -> String
                .take(2);                                                                      // 4. at most two names
    }

    /** Extension: same pipeline without take(2); collectList() turns Flux&lt;String&gt; into Mono&lt;List&lt;String&gt;&gt;. */
    static Mono<List<String>> allHighEarnerNames() {
        return employees()
                .filter(employee -> employee.salary() >= MIN_SALARY)
                .doOnNext(employee -> System.out.println("Passed filter: " + employee.name()))
                .map(employee -> employee.name().toUpperCase(Locale.ROOT))
                .collectList();
    }

    /**
     * Bonus - why doOnNext() <em>might</em> print more names than the subscriber receives.
     *
     * <p>In {@link #firstTwoHighEarnerNames()} everything is synchronous: take(2) caps the upstream
     * demand at 2 and cancels the source the moment the second name arrives, so doOnNext() only
     * sees Arun and Kumar.
     *
     * <p>Here publishOn() sits between doOnNext() and take(2). publishOn() hands elements over to
     * another thread and, to do that efficiently, it <em>prefetches</em>: on subscription it requests
     * 256 elements from upstream, whatever the downstream asked for. All four qualifying employees
     * therefore pass through doOnNext() into publishOn()'s queue before take(2) gets the chance
     * to cancel; the last two are then discarded.
     *
     * <p>To keep the console output deterministic, the "other thread" is an executor that only
     * queues the hand-off task; the task is run on this thread after subscribe() returns.
     */
    static void prefetchBeforeTakeDemo() {
        Queue<Runnable> pendingHandOffs = new ArrayDeque<>();
        Scheduler deferredScheduler = Schedulers.fromExecutor(pendingHandOffs::add);

        employees()
                .filter(employee -> employee.salary() >= MIN_SALARY)
                .doOnNext(employee -> System.out.println("Passed filter: " + employee.name()))
                .map(employee -> employee.name().toUpperCase(Locale.ROOT))
                .publishOn(deferredScheduler)
                .take(2)
                .subscribe(name -> System.out.println("Subscriber received: " + name));

        System.out.println("(publishOn hand-off to the subscriber runs now)");
        while (!pendingHandOffs.isEmpty()) {
            pendingHandOffs.poll().run();
        }
        deferredScheduler.dispose();
    }
}
