# Reactive Programming – Assignment 1: Solutions

**Stack:** Java 17+ · Project Reactor `reactor-core` 3.7.18 · JUnit 5 + `reactor-test` (StepVerifier)

All console output below is copied from a real run of `./run.sh`. The unit tests (`./run.sh test`, 18 tests) also assert every one of these outputs. How to build and run: see [README.md](README.md).

| # | Topic | Source (`src/main/java/com/ascend/reactive/`) | Test |
|---|---|---|---|
| 1 | Mono vs. Flux | `Exercise1MonoVsFlux.java` | `Exercise1MonoVsFluxTest` |
| 2 | Signals | `Exercise2Signals.java` | `Exercise2SignalsTest` |
| 3 | Laziness & re-subscription | `Exercise3Laziness.java` | `Exercise3LazinessTest` |
| 4 | First operators | `Exercise4Operators.java` | `Exercise4OperatorsTest` |
| 5 | Flux → Mono | `Exercise5FluxToMono.java` | `Exercise5FluxToMonoTest` |
| – | Claims made in these answers | – | `ReactorConceptsTest` |

All exercises share the record `model/Employee.java`: `record Employee(int id, String name, String department, int salary) {}`.

---

## Exercise 1 – Mono vs. Flux: creating publishers

### Code

```java
// 1. Mono<Employee> that emits one employee
Mono<Employee> employeeMono = Mono.just(new Employee(101, "Arun", "CSE", 65000));

// 2. Flux<Employee> that emits three employees
Flux<Employee> employeeFlux = Flux.just(
        new Employee(101, "Arun", "CSE", 65000),
        new Employee(102, "Priya", "ECE", 55000),
        new Employee(103, "Kumar", "CSE", 75000));

// 3. Subscribe to both and print the emitted employee objects
employeeMono.subscribe(
        employee -> System.out.println("Mono emitted : " + employee),
        error -> System.out.println("Mono error   : " + error.getMessage()),
        () -> System.out.println("Mono completed"));
employeeFlux.subscribe(
        employee -> System.out.println("Flux emitted : " + employee),
        error -> System.out.println("Flux error   : " + error.getMessage()),
        () -> System.out.println("Flux completed"));

// 4. Mono.empty() and Flux.empty()
Mono<Employee> emptyMono = Mono.empty();
Flux<Employee> emptyFlux = Flux.empty();

// 4a. value consumer only
emptyMono.subscribe(employee -> System.out.println("Empty Mono emitted: " + employee));
emptyFlux.subscribe(employee -> System.out.println("Empty Flux emitted: " + employee));

// 4b. also watch the subscription and the completion signal
emptyMono
        .doOnSubscribe(subscription -> System.out.println("Empty Mono subscribed"))
        .subscribe(
                employee -> System.out.println("Empty Mono emitted: " + employee),
                error -> System.out.println("Empty Mono error  : " + error.getMessage()),
                () -> System.out.println("Empty Mono completed without emitting any value"));
// (same for emptyFlux)
```

### Output

```
--- Task 3: subscribing to Mono<Employee> ---
Mono emitted : Employee[id=101, name=Arun, department=CSE, salary=65000]
Mono completed
--- Task 3: subscribing to Flux<Employee> ---
Flux emitted : Employee[id=101, name=Arun, department=CSE, salary=65000]
Flux emitted : Employee[id=102, name=Priya, department=ECE, salary=55000]
Flux emitted : Employee[id=103, name=Kumar, department=CSE, salary=75000]
Flux completed
--- Task 4a: Mono.empty() and Flux.empty() with a value consumer only ---
(nothing printed above: the value consumer is never called)
--- Task 4b: same publishers, also observing subscription and completion ---
Empty Mono subscribed
Empty Mono completed without emitting any value
Empty Flux subscribed
Empty Flux completed without emitting any value
```

**What happens with the empty publishers:** with only a value consumer, nothing is printed at all (4a). Adding `doOnSubscribe` and a completion callback (4b) explains why: the subscription does take place, and the publisher immediately sends `onComplete`. It never sends `onNext`.

### Answers

**How many items can a Mono emit?**
**At most one**: 0 or 1 `onNext`, then `onComplete` or `onError`.
- `Mono.just(x)`: 1 item, then complete.
- `Mono.empty()`: 0 items, then complete.
- `Mono.error(e)`: 0 items, then error.

A Mono can never carry two items. Forcing two into one, as in `Flux.just("Arun", "Priya").single()`, fails with `IndexOutOfBoundsException` (`ReactorConceptsTest`). Use a Mono for "one result or nothing": find-by-id, save, a single HTTP response.

**How many items can a Flux emit?**
**0 to N**, and N may be unbounded. Above, the Flux emitted 3 employees and then completed. `Flux.empty()` emits 0, and `Flux.interval(...)` emits forever without ever completing. Use a Flux for collections and streams: find-all, events, messages.

**What happens when a publisher completes without emitting any value?**
The subscriber receives `onSubscribe` and then directly `onComplete`:
- the value (`onNext`) callback is **never called**, which is why 4a printed nothing;
- it is **not an error**, so the error callback is not called either;
- the completion callback **is** called, which is why 4b printed "completed without emitting any value".

"Empty" is a valid, successful result, like a database query that matches no rows. To react to it, use `defaultIfEmpty(...)`, `switchIfEmpty(...)` or `hasElement()`. Blocking on an empty Mono with `block()` returns `null` (`ReactorConceptsTest`).

| | `Mono<T>` | `Flux<T>` |
|---|---|---|
| Items | 0..1 | 0..N (possibly infinite) |
| Signals | `onSubscribe → [onNext] → onComplete \| onError` | `onSubscribe → onNext* → onComplete \| onError` |
| Typical use | one result: findById, save, one HTTP response | many results: findAll, event streams |
| Empty version | `Mono.empty()`: only `onComplete` | `Flux.empty()`: only `onComplete` |

---

## Exercise 2 – Subscribing and understanding signals

### Code

```java
Mono<String> first = Mono.just("Employee found");
Mono<String> second = Mono.empty();
Mono<String> third =
        Mono.error(new RuntimeException("Employee not found"));

subscribeWithAllCallbacks("first  = Mono.just(\"Employee found\")", first);
subscribeWithAllCallbacks("second = Mono.empty()", second);
subscribeWithAllCallbacks("third  = Mono.error(...)", third);

private static void subscribeWithAllCallbacks(String label, Mono<String> publisher) {
    System.out.println("--- Subscribing to " + label + " ---");
    publisher.subscribe(
            value -> System.out.println("  onNext     -> value received: " + value),        // value
            error -> System.out.println("  onError    -> failed with: " + error.getMessage()), // error
            () -> System.out.println("  onComplete -> completed successfully"));            // completion
}

// Laziness check (supports question 3)
Mono<String> lookup = Mono.fromCallable(() -> {
    System.out.println("  [lookup] searching the employee records...");
    return "Employee found";
});
System.out.println("lookup Mono created - the search has NOT run yet");
lookup.subscribe(value -> System.out.println("  onNext     -> value received: " + value));
```

### Output

```
--- Subscribing to first  = Mono.just("Employee found") ---
  onNext     -> value received: Employee found
  onComplete -> completed successfully
--- Subscribing to second = Mono.empty() ---
  onComplete -> completed successfully
--- Subscribing to third  = Mono.error(...) ---
  onError    -> failed with: Employee not found
--- Laziness check: does anything run before subscribe()? ---
lookup Mono created - the search has NOT run yet
  [lookup] searching the employee records...
  onNext     -> value received: Employee found
```

### Which callbacks ran

| Publisher | value (`onNext`) | error (`onError`) | completion (`onComplete`) |
|---|:---:|:---:|:---:|
| `first = Mono.just("Employee found")` | ✅ | – | ✅ |
| `second = Mono.empty()` | – | – | ✅ |
| `third = Mono.error(...)` | – | ✅ | – |

### Answers

Every Reactive Streams publisher follows this protocol:

```
onSubscribe  onNext*  ( onComplete | onError )?
```

That means one `onSubscribe`, then **zero or more** `onNext`, then **at most one** terminal signal. The terminal signal is **either** `onComplete` **or** `onError`, never both.

**Why does `Mono.empty()` invoke the completion callback?**
`onComplete` means "the sequence finished successfully and no more items will come". It does not mean "a value was delivered". Because the protocol allows zero `onNext` signals, an empty sequence still counts as successfully finished. Reactive Streams rule 1.5 says a publisher that terminates successfully **must** signal `onComplete`. `Mono.empty()` finishes with no value and no failure, so the completion callback runs, and no other callback does. Without this signal the subscriber could not tell "finished with nothing" apart from "still waiting".

**Why does `Mono.error()` not invoke the completion callback?**
`onError` and `onComplete` are **mutually exclusive terminal signals**. A sequence ends exactly once: successfully (`onComplete`) or with a failure (`onError`, rule 1.4). Rule 1.7 says no signal may follow a terminal signal. Calling the "success" callback after a failure would report success for an operation that failed. If some code must run in both cases (closing a resource, stopping a timer), use `doFinally(signalType -> ...)` or `doOnTerminate(...)`.

> Always supply an error callback. Without one, Reactor 3.7.18 reports the failure through its default `onErrorDropped` hook:
> `[ERROR] (main) Operator called default onErrorDropped - reactor.core.Exceptions$ErrorCallbackNotImplemented: java.lang.RuntimeException: Employee not found`

**Does `subscribe()` have to be called to trigger normal subscription-driven processing?**
**Yes.** A `Mono` or `Flux` is only a *description* of work. Nothing runs (no signal, no `doOnNext`, no `map`, no callable) until a subscriber subscribes and requests data. The laziness check shows this: "lookup Mono created – the search has NOT run yet" is printed **before** "[lookup] searching…", which appears only after `subscribe()` is called. Exercise 3 shows the same behaviour.

Three nuances:
- **Something must subscribe, but it does not have to be your code.** `block()`, `blockFirst()`, `blockLast()`, `toFuture()`, `toIterable()` and `toStream()` all call `subscribe()` internally. Spring WebFlux subscribes to the `Mono`/`Flux` a controller returns.
- **Arguments are still evaluated eagerly.** `Mono.just(x)`, `Flux.just(...)` and `Mono.error(new RuntimeException(...))` evaluate their arguments when the line runs, so the exception object above exists before anyone subscribes. Only the *emission* waits for `subscribe()`. For lazy work, use `Mono.fromCallable(...)`, `Mono.defer(...)` or `Mono.error(Supplier)` (`ReactorConceptsTest`).
- **Hot publishers are the exception.** `Sinks` and `ConnectableFlux.connect()` emit regardless of subscribers. This is why the question says *normal* subscription-driven processing.

---

## Exercise 3 – Laziness and re-subscription

### Code (TODO completed)

```java
Flux<String> pipeline = Flux.just("Arun", "Priya", "Kumar")
        .doOnNext(name ->
                System.out.println("Processing: " + name));

System.out.println("Pipeline created");

// TODO (completed): subscribe to the pipeline twice
System.out.println("--- First subscription ---");
pipeline.subscribe(name -> System.out.println("Subscriber 1 received: " + name));

System.out.println("--- Second subscription ---");
pipeline.subscribe(name -> System.out.println("Subscriber 2 received: " + name));

// Bonus (supports answer 3)
Flux<String> cached = pipeline.cache();
cached.subscribe(name -> System.out.println("Cached subscriber 1 received: " + name));
cached.subscribe(name -> System.out.println("Cached subscriber 2 received: " + name));
```

### Output

```
Pipeline created
--- First subscription ---
Processing: Arun
Subscriber 1 received: Arun
Processing: Priya
Subscriber 1 received: Priya
Processing: Kumar
Subscriber 1 received: Kumar
--- Second subscription ---
Processing: Arun
Subscriber 2 received: Arun
Processing: Priya
Subscriber 2 received: Priya
Processing: Kumar
Subscriber 2 received: Kumar
--- Bonus: cache() shares a single execution ---
Processing: Arun
Cached subscriber 1 received: Arun
Processing: Priya
Cached subscriber 1 received: Priya
Processing: Kumar
Cached subscriber 1 received: Kumar
Cached subscriber 2 received: Arun
Cached subscriber 2 received: Priya
Cached subscriber 2 received: Kumar
```

### Answers

**1. What is printed when the pipeline is created?**
Only `Pipeline created`, and that line comes from the `println` placed *after* the pipeline. Building the pipeline (*assembly time*) prints nothing. `Flux.just(...).doOnNext(...)` only creates a chain of operator objects. The `doOnNext` lambda is stored for later, not executed. `Flux.just` captures the three names but does not emit them yet.

**2. When does "Processing: Arun" appear?**
Only when `subscribe()` is called (*subscription time*). It is the first line of the first subscription, after `Pipeline created`, and it is immediately followed by `Subscriber 1 received: Arun`. Each element travels through the whole chain (`doOnNext`, then the subscriber) before the next element is emitted. That is why "Processing" and "received" lines alternate instead of all "Processing" lines printing first. It appears again at the start of the second subscription.

**3. Why are the processing messages printed again for the second subscription?**
`Flux.just(...)` is a **cold** publisher. Every `subscribe()` creates a new, independent subscription that runs the **entire** pipeline again from the source, side effects included. The variable `pipeline` holds a *recipe*, not a stored result, so two subscriptions run the recipe twice. If the Flux wrapped a database query or an HTTP call, subscribing twice would execute the query or call twice.

To share one execution, make the pipeline shared or replaying with `cache()`, `share()` or `publish().autoConnect(n)`. The bonus output shows this: with `cache()` the "Processing" lines print only once, and the second subscriber receives the cached values.

---

## Exercise 4 – First operators: build a pipeline

### Code

```java
static Flux<Employee> employees() {
    return Flux.just(
            new Employee(101, "Arun", "CSE", 65000),
            new Employee(102, "Priya", "ECE", 55000),
            new Employee(103, "Kumar", "CSE", 75000),
            new Employee(104, "Divya", "IT", 80000),
            new Employee(105, "Ravi", "ECE", 72000));
}

static Flux<String> firstTwoHighEarnerNames() {
    return employees()
            .filter(employee -> employee.salary() >= MIN_SALARY)                           // 1. keep salary >= 60,000
            .doOnNext(employee -> System.out.println("Passed filter: " + employee.name())) // 2. side effect only
            .map(employee -> employee.name().toUpperCase(Locale.ROOT))                     // 3. Employee -> String
            .take(2);                                                                      // 4. at most two names
}

firstTwoHighEarnerNames()
        .subscribe(name -> System.out.println("Subscriber received: " + name));           // 5. subscribe & print

// Extension: take(2) removed, collectList() added
static Mono<List<String>> allHighEarnerNames() {
    return employees()
            .filter(employee -> employee.salary() >= MIN_SALARY)
            .doOnNext(employee -> System.out.println("Passed filter: " + employee.name()))
            .map(employee -> employee.name().toUpperCase(Locale.ROOT))
            .collectList();
}

allHighEarnerNames()
        .subscribe(names -> System.out.println("Collected list: " + names));
```

`MIN_SALARY` is `60_000`. `Locale.ROOT` makes the upper-casing independent of the machine's locale; in a Turkish locale, plain `"Divya".toUpperCase()` returns `"DİVYA"`.

### Output

```
--- Tasks 1-5: filter -> doOnNext -> map -> take(2) ---
Passed filter: Arun
Subscriber received: ARUN
Passed filter: Kumar
Subscriber received: KUMAR
--- Extension: take(2) removed, collectList() added ---
Passed filter: Arun
Passed filter: Kumar
Passed filter: Divya
Passed filter: Ravi
Collected list: [ARUN, KUMAR, DIVYA, RAVI]
--- Bonus: a case where doOnNext() prints MORE names than the subscriber gets ---
Passed filter: Arun
Passed filter: Kumar
Passed filter: Divya
Passed filter: Ravi
(publishOn hand-off to the subscriber runs now)
Subscriber received: ARUN
Subscriber received: KUMAR
```

Priya (₹55,000) never appears, because `filter()` drops her before she reaches `doOnNext()`.

### Answers

**Does `doOnNext()` modify an employee?**
**No.** `doOnNext` is a *side-effect* ("peek") operator. It runs a `Consumer<Employee>` (which returns nothing) for each element and passes the **same, unchanged** element downstream, so the type stays `Flux<Employee>`. Here nothing could be modified anyway: `Employee` is a `record`, so its fields are final and it has no setters. With a mutable object, a `doOnNext` *could* change it, but that is an anti-pattern; transformations belong in `map`.

**Does `map()` return an Employee or a String in this example?**
A **String**. `employee -> employee.name().toUpperCase(...)` is a `Function<Employee, String>`, so `map` turns the `Flux<Employee>` into a `Flux<String>`. That is why the subscriber prints `ARUN` and `KUMAR` rather than `Employee[...]` objects.

**What is the difference between `Flux<String>` and `Mono<List<String>>`?**

| | `Flux<String>` (tasks 1–5) | `Mono<List<String>>` (extension) |
|---|---|---|
| What is emitted | each name as its **own** `onNext` | **one** `onNext` carrying a `List` of all names |
| Subscriber callback runs | once per name (twice above) | once (`Collected list: [...]`) |
| When data arrives | each name as soon as it is ready | only after the source **completes** |
| Memory | one element at a time | all elements buffered in the list |
| `take`, `filter`, backpressure | act on individual names | act on the list as a whole |
| Empty source | only `onComplete` | emits an empty list `[]` |
| Error half-way | earlier names already delivered, then `onError` | no list at all, only `onError` |
| Infinite source | keeps emitting | never emits |

**Why might `doOnNext()` print more names than the final subscriber when it appears before `take(2)`?**
It comes down to *where* each operator sits. `doOnNext` runs for every element that reaches **its** position in the chain. `take(2)` sits further downstream, and the only way it can stop the flow is to **cancel the upstream after** its second element arrives. An element that has already left the source before that cancel still passes through `doOnNext`, and `take(2)` then discards it. So `doOnNext` reports what flowed past *its* position, not what the subscriber finally receives.

What we observed:
- **In this exercise's pipeline, `doOnNext` printed exactly the two names the subscriber received** (Arun, Kumar). Two things cause this. First, the pipeline is fully synchronous (`Flux.just`). Second, in the Reactor version used here (3.7.18; the default since 3.5), `take(n)` is `take(n, true)`: it caps the request sent upstream at *n* = 2 and cancels the moment the 2nd element arrives. `ReactorConceptsTest` verifies that the upstream request is exactly `[2]`, while `take(2, false)` requests `Long.MAX_VALUE`.
- **It prints more as soon as something upstream of `take` requests elements in advance.** Examples are prefetching or asynchronous operators (`publishOn`, `flatMap`, `concatMap`, `buffer`), `take(2, false)` on an asynchronous source, or the old pre-3.5 `take`. The **bonus** puts `publishOn` between `doOnNext` and `take(2)`. On subscription, `publishOn` prefetches 256 elements (`ReactorConceptsTest`). Result: `doOnNext` printed **4** names, but the subscriber got only **2**. In the demo, the scheduler queues its tasks and runs them after `subscribe()` returns, which keeps the output deterministic.

```java
employees()
        .filter(employee -> employee.salary() >= MIN_SALARY)
        .doOnNext(employee -> System.out.println("Passed filter: " + employee.name()))
        .map(employee -> employee.name().toUpperCase(Locale.ROOT))
        .publishOn(deferredScheduler)   // prefetches 256 elements from upstream
        .take(2)
        .subscribe(name -> System.out.println("Subscriber received: " + name));
```

Take-away: if a side effect (logging, auditing, saving, metrics) should apply only to the elements actually delivered, place it **after** `take()`.

---

## Exercise 5 – Moving between types: from Flux to Mono

### Code

```java
Flux<Employee> employees = Flux.just(
        new Employee(101, "Arun", "CSE", 65000),
        new Employee(102, "Priya", "ECE", 55000),
        new Employee(103, "Kumar", "CSE", 75000)
);

// 1-3. filter, map to an upper-case name, store in `names`
Flux<String> names = employees
        .filter(employee -> employee.salary() >= 60_000)             // Flux<Employee>
        .map(employee -> employee.name().toUpperCase(Locale.ROOT));  // Flux<String>

// 4. collect the names into one list
Mono<List<String>> nameList = names.collectList();

System.out.println("collectList() returned a " + nameList.getClass().getSimpleName()
        + ", not a java.util.List");

// 5. subscribe separately to both pipelines
names.subscribe(
        name -> System.out.println("names emitted    : " + name),
        error -> System.out.println("names failed     : " + error.getMessage()),
        () -> System.out.println("names completed"));

nameList.subscribe(
        list -> System.out.println("nameList emitted : " + list),
        error -> System.out.println("nameList failed  : " + error.getMessage()),
        () -> System.out.println("nameList completed"));

// Only at the edge of a program: wait for the Mono to get a plain List
List<String> plainList = nameList.block();
```

### Task 3: which declaration for `names`?

| Declaration | Compiles? | Reason |
|---|:---:|---|
| `Flux<Employee> names` | ❌ | after `map()` the elements are `String`s, not `Employee`s |
| **`Flux<String> names`** | ✅ | `filter`/`map` keep it a Flux (0..N items), and `map` made each item a `String` |
| `Mono<String> names` | ❌ | `filter`/`map` cannot turn a Flux (0..N) into a Mono (0..1); here there are two names |

**Task 4:** `Mono<List<String>> nameList = names.collectList();`

Compiler errors for the wrong choices (javac, Reactor 3.7.18):

```
error: incompatible types: inference variable V has incompatible bounds        // Flux<Employee> names = ...map(...)
    equality constraints: Employee
    lower bounds: String

error: incompatible types: no instance(s) of type variable(s) V exist so that Flux<V> conforms to Mono<String>   // Mono<String> names = ...map(...)

error: incompatible types: Mono<List<String>> cannot be converted to List<String>   // List<String> list = names.collectList();
```

### Output

```
collectList() returned a MonoCollectList, not a java.util.List
--- names (Flux<String>): one onNext per name ---
names emitted    : ARUN
names emitted    : KUMAR
names completed
--- nameList (Mono<List<String>>): ONE onNext carrying the whole list ---
nameList emitted : [ARUN, KUMAR]
nameList completed
nameList.block() returned a plain ArrayList: [ARUN, KUMAR]
```

### How the type changes

```
employees                                Flux<Employee>
  .filter(e -> e.salary() >= 60_000)     Flux<Employee>       same type, fewer elements
  .map(e -> e.name().toUpperCase())      Flux<String>         element type changes HERE
  .collectList()                         Mono<List<String>>   N elements become ONE list
```

### Answers

**At which operation does the element type change from Employee to String?**
At **`map()`**. `filter(Predicate<Employee>)` only decides whether each element passes, so it returns the same `Flux<Employee>`, just with fewer elements. `map(Function<? super T, ? extends R>)` returns a `Flux<R>`. With `employee -> employee.name().toUpperCase()`, `R` is `String`, so the result is a `Flux<String>`.

**Why does `collectList()` change the publisher type from `Flux<String>` to `Mono<List<String>>`?**
`collectList()` is an **aggregating** operator. It consumes *all* N elements and, when the source completes, produces **exactly one** result: a `List`. A single result means 0..1 items, which is a `Mono`, and the type of that one value is `List<String>`. The "many" moves from the publisher into the value. Consequences (all verified in `ReactorConceptsTest`):
- an empty source still emits one value, an **empty list** `[]`;
- an error in the source gives **no partial list**, only `onError`;
- an infinite source **never emits** a list.

**Does `collectList()` return a plain Java List immediately?**
**No.** It immediately returns a `Mono<List<String>>`; the output shows the object is a `MonoCollectList`. Writing `List<String> list = names.collectList();` does not even compile (`Mono<List<String>> cannot be converted to List<String>`). At that moment nothing has been collected. The list is built only when someone subscribes **and** the source completes, and every subscription builds a new list.

To get at the list:
- **Inside a reactive flow:** subscribe, or keep chaining operators: `nameList.subscribe(list -> ...)`, `nameList.map(...)`, `nameList.flatMap(...)`.
- **At the edge of a program** (a `main` method, tests): `nameList.block()` subscribes, waits, and returns the plain `ArrayList` `[ARUN, KUMAR]`. Never call `block()` inside a reactive pipeline. On Reactor's non-blocking threads (the `parallel` scheduler, Netty event loops) it throws `IllegalStateException` (`ReactorConceptsTest`).
