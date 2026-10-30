package com.ascend.reactive;

import com.ascend.reactive.model.Employee;
import java.util.List;
import java.util.Locale;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Exercise 5: moving between types - from Flux to Mono.
 *
 * <pre>
 *   employees                          Flux&lt;Employee&gt;
 *     .filter(salary &gt;= 60,000)        Flux&lt;Employee&gt;      (same type, fewer elements)
 *     .map(name -&gt; UPPERCASE)          Flux&lt;String&gt;        (element type changes here)
 *     .collectList()                   Mono&lt;List&lt;String&gt;&gt;  (N elements become ONE list)
 * </pre>
 */
public final class Exercise5FluxToMono {

    private Exercise5FluxToMono() {
    }

    public static void main(String[] args) {
        run();
    }

    public static void run() {
        Flux<Employee> employees = Flux.just(
                new Employee(101, "Arun", "CSE", 65000),
                new Employee(102, "Priya", "ECE", 55000),
                new Employee(103, "Kumar", "CSE", 75000)
        );

        // Tasks 1-3: filter() keeps Employee elements, map() turns each one into a String.
        Flux<String> names = employees
                .filter(employee -> employee.salary() >= 60_000)             // Flux<Employee>
                .map(employee -> employee.name().toUpperCase(Locale.ROOT));  // Flux<String>

        // Rejected declarations (both fail to compile):
        //   Flux<Employee> names = ...  -> after map() the elements are Strings, not Employees
        //   Mono<String>   names = ...  -> filter()/map() never turn a Flux (0..N) into a Mono (0..1)

        // Task 4: collectList() gathers ALL names into ONE list and emits it once.
        Mono<List<String>> nameList = names.collectList();

        // collectList() returned a lazy publisher, not a java.util.List.
        System.out.println("collectList() returned a " + nameList.getClass().getSimpleName()
                + ", not a java.util.List");

        // Task 5: subscribe separately to both pipelines.
        System.out.println("--- names (Flux<String>): one onNext per name ---");
        names.subscribe(
                name -> System.out.println("names emitted    : " + name),
                error -> System.out.println("names failed     : " + error.getMessage()),
                () -> System.out.println("names completed"));

        System.out.println("--- nameList (Mono<List<String>>): ONE onNext carrying the whole list ---");
        nameList.subscribe(
                list -> System.out.println("nameList emitted : " + list),
                error -> System.out.println("nameList failed  : " + error.getMessage()),
                () -> System.out.println("nameList completed"));

        // To obtain a plain List you must wait for the Mono. block() subscribes and waits; acceptable
        // at the edge of a program (main method, tests) but never inside a reactive pipeline.
        List<String> plainList = nameList.block();
        System.out.println("nameList.block() returned a plain " + plainList.getClass().getSimpleName()
                + ": " + plainList);
    }
}
