Onesource
=========

Text processing pipeline.

Text is processed in stages (Steps), which can be added to a Pipeline to run in sequence.

Running a Step via a Pipeline provides automatic logging and state tracking for restarts.

Under `onesource` is version 1 of the code. Processing steps are added to a pipeline, which
is then run. Each step is completed before the next step begins.

Under `v2` is starting a new version, which uses workers that can run in parallel. Dependencies
are managed at the file level. As soon as one file is processed by a step, the next step
for that file can begin without waiting for all files to be processed by a given step.

`v2` is in development.

To run v1, call `onesource/__init__.py` with the following arguments:
  --read read root dir
  --write write root dir
  --temp temp dir (for control files)
  --overwrite overwrite existing files
