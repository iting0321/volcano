# ExpectedPartitions Prototype

## Goal

Provide a focused prototype for `ExpectedPartitions` on top of the existing flat sub-job model, without introducing hierarchical sub-job trees yet.

For a task with:

- `totalPartitions = 4`
- `partitionSize = 2`
- `minPartitions = 1`
- `expectedPartitions = [1, 2, 4]`

the scheduler may commit the job only at partition counts `1`, `2`, or `4`. It must not commit the intermediate count `3`.

## API Shape

- `batch.Job.spec.tasks[].partitionPolicy.expectedPartitions`
- `scheduling.PodGroup.spec.subGroupPolicy[].expectedSubGroups`

The job controller copies `ExpectedPartitions` into the corresponding PodGroup `ExpectedSubGroups`.

## Scheduler Semantics

The prototype keeps the current leaf partition sub-jobs and changes only the target count logic:

1. Each partition still maps to one flat sub-job.
2. The scheduler counts how many sub-jobs in a subgroup policy are already ready.
3. If `ExpectedSubGroups` is configured, the next scheduling target becomes the smallest expected count greater than the current ready count.
4. A job is considered subgroup-ready only when the ready subgroup count reaches that target exactly.

This produces stepwise progression such as:

- `0 -> 1`
- `1 -> 2`
- `2 -> 4`

and naturally rejects `2 -> 3` as a committed state.

## Non-Goals

This prototype does not implement:

- hierarchical parent/child subgroup trees
- preempt integration for exact target recovery
- reclaim integration for exact target recovery
- reservation across multiple future steps

Those remain follow-up work for the full design.
