# Spark tuning knowledge base

## Executor memory

Executor heap recommendations must account for container or worker limits and non-heap overhead. More heap can reduce spill, but it may reduce parallelism if the same cluster must host fewer executors.

## Shuffle partitions

Partition count should be treated as a starting point rather than a universal optimum. Very small partitions increase scheduling overhead; very large partitions increase memory pressure and straggler risk. Adaptive Query Execution can coalesce partitions at runtime.

## Adaptive Query Execution

AQE can revise a physical query plan using runtime statistics. Its effect depends on workload structure, Spark version and data distribution.

## Serialization

Kryo can reduce serialization cost for many object-heavy workloads. It is not guaranteed to outperform all encoders and may require registration or buffer tuning.
