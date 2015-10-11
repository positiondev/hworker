## About

`hworker` is a Redis-backed persistent at-least-once queue library. It
is vaguely inspired by `sidekiq` for Ruby. It is intended to be a
simple reliable mechanism for processing background tasks. The jobs
can be created by a Haskell application or any application that can
push JSON data structures of the right shape into a Redis queue.

## Stability

This has been running in one application sending email (using
`hworker-ses`) for several months. This is relatively low traffic
(transactional messages) most of the time, with spikes of 10k-30k
messages (mailing blasts).

## Important Note

The expiration of jobs is really important. It defaults to 120
seconds, which may be short depending on your application (for things
like sending emails, it may be fine). **The reason why this timeout is
important is that if a job ever runs longer than this, the monitor
will think that the job failed** in some inexplicable way (like the
server running the job died) and will add the job back to the queue to
be run. Based on the semantics of this job processor, jobs running
multiple times is not a failure case, but it's obviously not something
you _want_ to happen, so be sure to set the timeout to something
reasonable for your application.

## Overview

To define jobs, you define a serialized representation of the job, and
a function that runs the job, which returns a status. The behavior of
uncaught exceptions is defined when you create the worker - it can be
either `Failure` or `Retry`. Jobs that return `Failure` are removed
from the queue, whereas jobs that return `Retry` are added again. The
only difference between a `Success` and a `Failure` is that a
`Failure` returns a message that is logged (ie, neither run again).

## Semantics

This behavior of this queue processor is at-least-once.

We rely on the defined behavior of Redis for reliability. Once a job
has been `queue`d, it is guaranteed to be run eventually, provided
some worker and monitor threads exist. If the worker thread that was
running a given job dies, the job will eventually be retried (if you
do not want this behavior, do not start any monitor threads). Once the
job completes, provided nothing kills the worker thread in the
intervening time, jobs that returned `Success` will not be run again,
jobs that return `Failure` will have their messages logged and will
not be run again, and jobs that return `Retry` will be queued
again. If something kills the worker thread before these
acknowledgements go through, the job will be retried. Exceptions
triggered within the job cannot affect the worker thread - what they
do to the job is defined at startup (they can cause either a `Failure`
or `Retry`).

Any deviations from this behavior are considered bugs that will be fixed.


## Redis Operations

Under the hood, we will have the following data structures in redis
(`name` is set when you create the `hworker` instance):

```
hworker-jobs-name: list of json serialized job descriptions

hworker-progress-name: a hash of jobs that are in progress, mapping to time started

hworker-broken-name: a hash of jobs to time that couldn't be deserialized; most likely means you changed the serialization format with jobs still in queue, _or_ you pointed different applications at the same queues.

hworker-failed-queue: a record of the jobs that failed (limited in size based on config).
```

In the following pseudo-code, I'm using `MULTI`...`EXEC` to indicate
atomic blocks of code. These are actually implemented with lua and
`EVAL`, but I think it's easier to read this way. If you want to see
what's actually happening, just read the code - it's not very long!

When a worker wants to do work, the following happens:

```
now = TIME
MULTI
v = RPOP hworker-jobs-name
if v
  HSET hworker-progress-name v now
EXEC
v
```

When it completes the job, it does the following:

```
v = JOB
HDEL hwork-progress v
```

If the job returned `Retry`, the following occurs:

```
v = JOB
t = START_TIME
MULTI
LPUSH hwork-jobs v
HDEL hwork-progress t
EXEC
```

A monitor runs on another thread that will re-run jobs that stay in
progress for too long (as that indicates that something unknown went
wrong). The operation that it runs periodically is:

```
keys = HKEYS (or HSCAN) hwork-progress
for keys as v:
  started = HGET hwork-progress v
  if started < TIME - timeout
    MULTI
    RPUSH hwork-jobs v
    HDEL hwork-progress v
    EXEC
```

Note that what the monitor does and `Retry` is slightly different -
the monitor puts jobs on the front of the queue, whereas `Retry` puts
them on the back.

## Primary Libraries Used

- hedis
- aeson

## Contributors

- Daniel Patterson (@dbp - dbp@dbpmail.net)

## Build Status

[![Circle CI](https://circleci.com/gh/dbp/hworker.svg?style=svg&circle-token=b40a5b06c599d457cbaa4d1c00824c98d4768f2f)](https://circleci.com/gh/dbp/hworker)
