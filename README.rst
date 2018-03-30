Onesource
=========

Text processing pipeline.

Text is processed in stages (Steps), which can be added to a Pipeline to run in sequence.

Running a Step via a Pipeline provides automatic logging and state tracking for restarts.

Under ``onesource`` is version 1 of the code. Processing steps are added to a pipeline, which
is then run. Each step is completed before the next step begins.

Under ``v2`` is starting a new version, which uses workers that can run in parallel. Dependencies
are managed at the file level. As soon as one file is processed by a step, the next step
for that file can begin without waiting for all files to be processed by a given step.

``v2`` is in development.

To run v1, call ``onesource/__init__.py`` with the following arguments::

    ``--read`` read root dir
    ``--write`` write root dir
    ``--temp temp`` dir (for control files)
    ``--overwrite`` overwrite existing files

There is a startup penalty with v2, but outperforms with scale. v2 is using pure functions.
An hypothesis is that given document parsing is stateful, which is requiring incremental
copies of accumulating data structures to remain immutable, performance will be improved
with a hybrid approach - parallel tasks using mutable data structures within each task.

* 2 files:
    * Sequential pipeline (v1) performance - elapsed: 0.0938s
    * Parallel workers (v2) performance - elapsed: 0.6873s
* 21 files
    * Sequential pipeline (v1) performance - elapsed: 1.1118s
    * Parallel workers (v2) performance - elapsed: 0.9113s

