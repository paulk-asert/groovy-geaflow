# groovy-geaflow

Examples exploring [Apache GeaFlow (incubating)](https://geaflow.apache.org/)
from Groovy, accompanying the
[Graph Computing with Groovy and Apache GeaFlow](https://groovy.apache.org/blog/groovy-geaflow)
blog post.

## Running

`./gradlew runPageRank` runs a Groovy vertex-centric PageRank job
(ranking swimmers in a hypothetical social network by influence)
on the local in-process environment
(`EnvironmentFactory.onLocalEnvironment()`) with an in-memory source,
completing in ~5s.

`./gradlew runVariant -Pvariant=lambda|methodref|closure|coerce` submits the
same job written in different Groovy styles, to explore which ones survive
kryo serialization across GeaFlow's RPC boundary (see findings below).

* GeaFlow 0.8.0-incubating (Maven Central, `org.apache.geaflow`)
* Groovy 6.0.0-beta-2, Gradle toolchain JDK 17

## Findings so far

1. **JDK 9+ needs `--add-opens`.** GeaFlow's RPC layer (baidu brpc) uses cglib,
   which needs `--add-opens=java.base/java.lang=ALL-UNNAMED` (see `build.gradle`
   for the set used). Without it, local mode *hangs* rather than fails:
   the first container→master RPC throws `InaccessibleObjectException`, which is
   swallowed by a retry loop, and the failure also evicts the `_MASTER` entry from
   the in-memory HA/discovery cache, which is never repopulated — so every
   subsequent heartbeat logs `Resource data not found for resource: _MASTER`
   forever. (Possibly worth an upstream issue: log the swallowed exception on
   first retry, and re-resolve after invalidation in `MemoryHAService`.)
2. **Functions crossing the RPC boundary must be kryo-serializable.**
   Tested with `SubmitVariants.groovy` (`gradle runVariant -Pvariant=...`),
   all under `@CompileStatic`, submitting the same job as a `PipelineTask`:

   | Form | Runtime class | Result (Groovy 6.0.0-beta-2) |
   |---|---|---|
   | class implementing `PipelineTask` | normal class | works |
   | native lambda `(ctx) -> runJob(ctx)` | indy lambda (serializable) | **works** |
   | method ref `SubmitVariants::runJob` | indy lambda (serializable) | **works** (Groovy 6+ only, see below) |
   | closure `{ ctx -> runJob(ctx) }` (implicit SAM) | `jdk.proxy` | fails at kryo read: `InstantiationError: InvocationHandler` |
   | closure `as PipelineTask` | `jdk.proxy` | fails at kryo read (same) |

   So the Groovy sweet spot is: hoist the body into a (static) method and wrap it
   in a **native lambda expression** or **method reference** — GeaFlow registers
   kryo's `ClosureSerializer`, which round-trips serializable indy lambdas via
   `SerializedLambda`. Lambda captures must themselves be serializable;
   a no-capture static context is safest.

   Two upstream-worthy observations from this:
   * **Method references need Groovy 6.** In Groovy 4.x/5.x (and 6.0.0-alpha-1),
     with static compilation, a *method reference* assigned to a `Serializable`
     functional interface is not compiled as a serializable lambda (plain
     `ObjectOutputStream` throws `NotSerializableException: Non-serializable
     lambda`), while the equivalent *lambda expression* is. Java makes both
     serializable (`repro/SerCheck.groovy`, no GeaFlow needed). Fixed by
     [GROOVY-11993](https://issues.apache.org/jira/browse/GROOVY-11993)
     "Support serializable method reference" from 6.0.0-beta-2: the standalone
     round-trip (serialize + deserialize + invoke) passes, and the `methodref`
     variant succeeds end-to-end through GeaFlow's kryo/RPC path.
     Groovy 5.x still affected — possible backport candidate.
   * **GEP-27 flags don't change the picture** (tested on beta-2 with
     `-Pgep27` → `-Dgroovy.target.closure.pack=true -Dgroovy.target.lambda.hoist=true`
     in the forked compiler): closure→SAM coercion still yields a `jdk.proxy`
     (packing hoists the closure *body*, not the coercion mechanism), so the kryo
     failure remains; and compact/hoisted lambda compilation explicitly exempts
     serializable SAM targets, so the working lambda path is unchanged.
   * The closure→SAM `jdk.proxy` objects are actually *Java*-serializable
     (`ConvertedClosure` handler is serializable) — they only fail because kryo's
     `FieldSerializer` tries to instantiate `InvocationHandler` reflectively.
     GeaFlow could support them by registering a proxy-aware kryo serializer.
     In dynamic (non-`@CompileStatic`) code, lambdas and method refs *also*
     become proxies, so they'd fail with kryo too.
3. **Static type checking needs a few explicit types.** The fluent API's default
   methods return raw types (`PWindowStream`), so `@CompileStatic` code should
   declare `PWindowStream<...>`/`PGraphWindow<K,VV,EV>` locals explicitly to keep
   generics flowing.
4. Vertex-centric compute (Pregel-style) ports to Groovy cleanly; Groovy niceties
   that show well: ranges/collect for building vertices/edges, list-of-pairs for
   edges, `messages.sum()`, Groovy truth for `outEdges`, named-arg constructors.
