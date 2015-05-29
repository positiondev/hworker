## Design

- Redis for reliable thread-safe queue.
- Custom jobs queued and run.
- Re-running on failure.
- Can run standalone or fork in thread.

## Libraries

hedis
binary

## Implementation

```
data Result = Success | Retry Text | Failure Text
instance Binary Result
  ...

class Binary t => Job t where
  job :: t -> (t -> IO Result)
  ran :: t -> (Result -> IO ())
  ran _ = const (return ())

data Job = Job UTCTime t -- contains time started, so jobs are more unique.

queue :: Job t => t -> IO ()
```

```
data Complete = forall t, Job t => Complete t Result
instance Job Complete where
  job (Complete t r) = catch (ran t r >> return Success)
    (\someexception -> return (Failure (show ...)))
```

To define jobs, you define whatever serialized representation of the
job, and a function that runs the job, which returns a status. Any
uncaught exceptions will be treated as `Failure`, so `Retry` should be
used explicitly if needed.

Under the hood, we will have the following data structures in redis:

```
hwork-jobs-${project}: list of binary serialized job descriptions
hwork-progress-${project}: a hash of jobs that are in progress, mapping to time started
```

When a worker wants to do work, the following happens in redis:

```
MULTI
v = RPOP hwork-jobs
now = TIME
HSET hwork-progress v now
EXEC
v
```

When it completes the job, it does the following:

```
v = JOB
c = COMPLETEDJOB
MULTI
HDEL hwork-progress v
RPUSH hwork-jobs c
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
