# Storage Tutorial

This track is the guided path for learning `CSharpDB.Storage`.

It is organized from mental model to extension surface to executable examples:

1. [Architecture](./architecture.md)
2. [Usage and extensibility](./extensibility.md)
3. [Runnable examples index](./examples/README.md)

## When to use this track

Use this track if you want to understand:

- how `Pager`, WAL, B+tree, and `SchemaCatalog` fit together
- which parts of the storage stack are configurable today
- how to experiment with those extension points in executable code
- how to use the storage engine for non-traditional workloads (file systems, config stores, event logs, job queues, graph databases)

## Runnable example tracks

The runnable examples are split into two branches:

- **Study examples**: the shared `StorageStudyExamples.*` REPL-based walkthroughs for learning storage concepts in smaller guided slices
- **Advanced standalone examples**: richer domain applications built directly on `CSharpDB.Storage`, including graph, spatial, time-series, and virtual file system engines

Start with the study examples if you are new to the storage surface. Jump to the advanced standalone examples when you want larger end-to-end samples that show how to turn the storage package into a domain-specific engine.

## Related material

- [Storage reference walkthrough](../../storage/README.md)
- [Storage package README](../../../src/CSharpDB.Storage/README.md)
