{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}


--------------------------------------------------------------------------------
import           Control.Concurrent       ( forkIO, killThread, threadDelay )
import           Control.Concurrent.MVar  ( MVar, modifyMVarMasked_, newMVar
                                          , readMVar, takeMVar
                                          )
import           Control.Monad            ( replicateM_, void )
import           Control.Monad.Trans      ( lift, liftIO )
import           Data.Aeson               ( FromJSON(..), ToJSON(..) )
import qualified Data.Conduit            as Conduit
import           Data.Text                ( Text )
import qualified Data.Text               as T
import           Data.Time
import qualified Database.Redis          as Redis
import           GHC.Generics             ( Generic)
import           Test.Hspec
import           Test.HUnit               ( assertEqual )
--------------------------------------------------------------------------------
import           System.Hworker
--------------------------------------------------------------------------------



main :: IO ()
main = hspec $ do
  describe "Simple" $ do
    it "should run and increment counter" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      wthread <- forkIO (worker hworker)
      queue hworker SimpleJob
      threadDelay 30000
      killThread wthread
      destroy hworker
      v <- takeMVar mvar
      assertEqual "State should be 1 after job runs" 1 v

    it "queueing 2 jobs should increment twice" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-2" (SimpleState mvar))
      wthread <- forkIO (worker hworker)
      queue hworker SimpleJob
      queue hworker SimpleJob
      threadDelay 40000
      killThread wthread
      destroy hworker
      v <- takeMVar mvar
      assertEqual "State should be 2 after 2 jobs run" 2 v

    it "queueing 1000 jobs should increment 1000" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-3" (SimpleState mvar))
      wthread <- forkIO (worker hworker)
      replicateM_ 1000 (queue hworker SimpleJob)
      threadDelay 2000000
      killThread wthread
      destroy hworker
      v <- takeMVar mvar
      assertEqual "State should be 1000 after 1000 job runs" 1000 v

    it "should work with multiple workers" $ do
      -- NOTE(dbp 2015-07-12): This probably won't run faster, because
      -- they are all blocking on the MVar, but that's not the point.
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-4" (SimpleState mvar))
      wthread1 <- forkIO (worker hworker)
      wthread2 <- forkIO (worker hworker)
      wthread3 <- forkIO (worker hworker)
      wthread4 <- forkIO (worker hworker)
      replicateM_ 1000 (queue hworker SimpleJob)
      threadDelay 1000000
      killThread wthread1
      killThread wthread2
      killThread wthread3
      killThread wthread4
      destroy hworker
      v <- takeMVar mvar
      assertEqual "State should be 1000 after 1000 job runs" 1000 v

  describe "Exceptions" $ do
    it "should be able to have exceptions thrown in jobs and retry the job" $ do
      mvar <- newMVar 0
      hworker <-
        createWith
          (conf "exworker-1" (ExState mvar))
            { hwconfigExceptionBehavior = RetryOnException }
      wthread <- forkIO (worker hworker)
      queue hworker ExJob
      threadDelay 40000
      killThread wthread
      destroy hworker
      v <- takeMVar mvar
      assertEqual "State should be 2, since the first run failed" 2 v

    it "should not retry if mode is FailOnException" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "exworker-2" (ExState mvar))
      wthread <- forkIO (worker hworker)
      queue hworker ExJob
      threadDelay 30000
      killThread wthread
      destroy hworker
      v <- takeMVar mvar
      assertEqual "State should be 1, since failing run wasn't retried" 1 v

  describe "Retry" $ do
    it "should be able to return Retry and get run again" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "retryworker-1" (RetryState mvar))
      wthread <- forkIO (worker hworker)
      queue hworker RetryJob
      threadDelay 50000
      killThread wthread
      destroy hworker
      v <- takeMVar mvar
      assertEqual "State should be 2, since it got retried" 2 v

  describe "Fail" $ do
    it "should not retry a job that Fails" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "failworker-1" (FailState mvar))
      wthread <- forkIO (worker hworker)
      queue hworker FailJob
      threadDelay 30000
      killThread wthread
      destroy hworker
      v <- takeMVar mvar
      assertEqual "State should be 1, since failing run wasn't retried" 1 v

    it "should put a failed job into the failed queue" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "failworker-2" (FailState mvar))
      wthread <- forkIO (worker hworker)
      queue hworker FailJob
      threadDelay 30000
      killThread wthread
      failedJobs <- failed hworker
      destroy hworker
      assertEqual "Should have failed job" [FailJob] failedJobs

    it "should only store failedQueueSize failed jobs" $ do
      mvar <- newMVar 0
      hworker <-
        createWith
          (conf "failworker-3" (AlwaysFailState mvar))
            { hwconfigFailedQueueSize = 2 }
      wthread <- forkIO (worker hworker)
      queue hworker AlwaysFailJob
      queue hworker AlwaysFailJob
      queue hworker AlwaysFailJob
      queue hworker AlwaysFailJob
      threadDelay 100000
      killThread wthread
      failedJobs <- failed hworker
      destroy hworker
      v <- takeMVar mvar
      assertEqual "State should be 4, since all jobs were run" 4 v
      assertEqual "Should only have stored 2" [AlwaysFailJob,AlwaysFailJob] failedJobs

  describe "Batch" $ do
    it "should set up a batch job" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      summary <- startBatch hworker Nothing >>= expectBatchSummary hworker
      batchSummaryQueued summary `shouldBe` 0
      batchSummaryCompleted summary `shouldBe` 0
      batchSummarySuccesses summary `shouldBe` 0
      batchSummaryFailures summary `shouldBe` 0
      batchSummaryRetries summary `shouldBe` 0
      batchSummaryStatus summary `shouldBe` BatchQueueing
      destroy hworker

    it "should expire batch job" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      batch <- startBatch hworker (Just 1)
      batchSummary hworker batch >>= shouldNotBe Nothing
      threadDelay 2000000
      batchSummary hworker batch >>= shouldBe Nothing
      destroy hworker

    it "should increment batch total after queueing a batch job" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      batch <- startBatch hworker Nothing
      queueBatch hworker batch False [SimpleJob]
      summary <- expectBatchSummary hworker batch
      batchSummaryQueued summary `shouldBe` 1
      destroy hworker

    it "should not enqueue job for completed batch" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      wthread <- forkIO (worker hworker)
      batch <- startBatch hworker Nothing
      queueBatch hworker batch False [SimpleJob]
      threadDelay 30000
      stopBatchQueueing hworker batch
      summary <- expectBatchSummary hworker batch
      queueBatch hworker batch False [SimpleJob]
        >>= shouldBe (AlreadyQueued summary)
      threadDelay 30000
      summary' <- expectBatchSummary hworker batch
      batchSummaryQueued summary' `shouldBe` 1
      killThread wthread
      destroy hworker

    it "should increment success and completed after completing a successful batch job" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      wthread <- forkIO (worker hworker)
      batch <- startBatch hworker Nothing
      queueBatch hworker batch False [SimpleJob]
      threadDelay 30000
      summary <- expectBatchSummary hworker batch
      batchSummaryQueued summary `shouldBe` 1
      batchSummaryFailures summary `shouldBe` 0
      batchSummarySuccesses summary `shouldBe` 1
      batchSummaryCompleted summary `shouldBe` 1
      batchSummaryStatus summary `shouldBe` BatchQueueing
      killThread wthread
      destroy hworker

    it "should increment failure and completed after completing a failed batch job" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "failworker-1" (FailState mvar))
      wthread <- forkIO (worker hworker)
      batch <- startBatch hworker Nothing
      queueBatch hworker batch False [FailJob]
      threadDelay 30000
      summary <- expectBatchSummary hworker batch
      batchSummaryQueued summary `shouldBe` 1
      batchSummaryFailures summary `shouldBe` 1
      batchSummarySuccesses summary `shouldBe` 0
      batchSummaryCompleted summary `shouldBe` 1
      batchSummaryStatus summary `shouldBe` BatchQueueing
      killThread wthread
      destroy hworker

    it "should change job status to processing when batch is set to stop queueing" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      batch <- startBatch hworker Nothing
      queueBatch hworker batch False [SimpleJob]
      stopBatchQueueing hworker batch
      summary <- expectBatchSummary hworker batch
      batchSummaryQueued summary `shouldBe` 1
      batchSummaryStatus summary `shouldBe` BatchProcessing
      destroy hworker

    it "should change job status to finished when batch is set to stop queueing and jobs are already run" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      wthread <- forkIO (worker hworker)
      batch <- startBatch hworker Nothing
      queueBatch hworker batch False [SimpleJob]
      threadDelay 30000
      stopBatchQueueing hworker batch
      Just batch' <- batchSummary hworker batch
      batchSummaryQueued batch' `shouldBe` 1
      batchSummaryStatus batch' `shouldBe` BatchFinished
      killThread wthread
      destroy hworker

    it "should change job status to finished when last processed" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      wthread <- forkIO (worker hworker)
      batch <- startBatch hworker Nothing
      queueBatch hworker batch False [SimpleJob]
      stopBatchQueueing hworker batch
      threadDelay 30000
      summary <- expectBatchSummary hworker batch
      batchSummaryQueued summary `shouldBe` 1
      batchSummaryStatus summary `shouldBe` BatchFinished
      killThread wthread
      destroy hworker

    it "queueing 1000 jobs should increment 1000" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-3" (SimpleState mvar))
      wthread <- forkIO (worker hworker)
      batch <- startBatch hworker Nothing
      queueBatch hworker batch False (replicate 1000 SimpleJob)
      stopBatchQueueing hworker batch
      threadDelay 2000000
      v <- takeMVar mvar
      v `shouldBe` 1000
      summary <- expectBatchSummary hworker batch
      batchSummaryQueued summary `shouldBe` 1000
      batchSummaryFailures summary `shouldBe` 0
      batchSummarySuccesses summary `shouldBe` 1000
      batchSummaryCompleted summary `shouldBe` 1000
      batchSummaryStatus summary `shouldBe` BatchFinished
      killThread wthread
      destroy hworker

    describe "Atomicity Tests" $ do
      it "should queue all jobs" $ do
        mvar <- newMVar 0
        hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
        batch <- startBatch hworker Nothing
        streamBatch hworker batch True
          $ replicateM_ 50
          $ Conduit.yield (QueueJob SimpleJob)
        ls <- jobs hworker
        length ls `shouldBe` 50
        summary <- expectBatchSummary hworker batch
        batchSummaryQueued summary `shouldBe` 50
        batchSummaryStatus summary `shouldBe` BatchProcessing
        destroy hworker

      it "should not queue jobs when producer throws error" $ do
        mvar <- newMVar 0
        hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
        batch <- startBatch hworker Nothing
        streamBatch hworker batch True $ do
          replicateM_ 20 $ Conduit.yield (QueueJob SimpleJob)
          Conduit.yield (AbortQueueing "abort")
          replicateM_ 20 $ Conduit.yield (QueueJob SimpleJob)
        ls <- jobs hworker
        expectBatchSummary hworker batch
        destroy hworker
        length ls `shouldBe` 0

      it "should not queue jobs on transaction error" $ do
        mvar <- newMVar 0
        hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
        batch <- startBatch hworker Nothing
        streamBatch hworker batch True $ do
          replicateM_ 20 $ Conduit.yield (QueueJob SimpleJob)
          _ <- lift $ Redis.lpush "" []
          replicateM_ 20 $ Conduit.yield (QueueJob SimpleJob)
        ls <- jobs hworker
        destroy hworker
        length ls `shouldBe` 0

      it "should not queue jobs when transaction is aborted" $ do
        mvar <- newMVar 0
        hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
        batch <- startBatch hworker Nothing
        _ <- Redis.runRedis (hworkerConnection hworker) $ Redis.watch [batchCounter hworker batch]
        streamBatch hworker batch True $ replicateM_ 20 $ Conduit.yield (QueueJob SimpleJob)
        ls <- jobs hworker
        destroy hworker
        length ls `shouldBe` 0

      it "should increment summary up until failure" $ do
        mvar <- newMVar 0
        hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
        batch <- startBatch hworker Nothing

        thread <-
          forkIO . void . streamBatch hworker batch True $ do
            replicateM_ 5 $
              Conduit.yield (QueueJob SimpleJob) >> liftIO (threadDelay 50000)
            error "BLOW UP!"
            replicateM_ 5 $
              Conduit.yield (QueueJob SimpleJob) >> liftIO (threadDelay 50000)

        threadDelay 190000
        summary1 <- expectBatchSummary hworker batch
        batchSummaryQueued summary1 `shouldBe` 4
        ls <- jobs hworker
        length ls `shouldBe` 0
        threadDelay 100000
        summary2 <- expectBatchSummary hworker batch
        batchSummaryQueued summary2 `shouldBe` 5
        batchSummaryStatus summary2 `shouldBe` BatchFailed
        killThread thread
        destroy hworker


  describe "Monitor" $ do
    it "should add job back after timeout" $ do
      -- NOTE(dbp 2015-07-12): The timing on this test is somewhat
      -- tricky.  We want to get the job started with one worker,
      -- then kill the worker, then start a new worker, and have
      -- the monitor put the job back in the queue and have the
      -- second worker finish it. It's important that the job
      -- takes less time to complete than the timeout for the
      -- monitor, or else it'll queue it forever.
      --
      -- The timeout is 5 seconds. The job takes 1 seconds to run.
      -- The worker is killed after 0.5 seconds, which should be
      -- plenty of time for it to have started the job. Then after
      -- the second worker is started, we wait 10 seconds, which
      -- should be plenty; we expect the total run to take around 11.
      mvar <- newMVar 0
      hworker <-
        createWith
          (conf "timedworker-1" (TimedState mvar)) { hwconfigTimeout = 5 }
      wthread1 <- forkIO (worker hworker)
      mthread <- forkIO (monitor hworker)
      queue hworker (TimedJob 1000000)
      threadDelay 500000
      killThread wthread1
      wthread2 <- forkIO (worker hworker)
      threadDelay 10000000
      v <- takeMVar mvar
      killThread wthread2
      killThread mthread
      destroy hworker
      assertEqual "State should be 1, since first failed" 1 v

    it "should add back multiple jobs after timeout" $ do
      -- NOTE(dbp 2015-07-23): Similar to the above test, but we
      -- have multiple jobs started, multiple workers killed.
      -- then one worker will finish both interrupted jobs.
      mvar <- newMVar 0
      hworker <-
        createWith
          (conf "timedworker-2" (TimedState mvar)) { hwconfigTimeout = 5 }
      wthread1 <- forkIO (worker hworker)
      wthread2 <- forkIO (worker hworker)
      mthread <- forkIO (monitor hworker)
      queue hworker (TimedJob 1000000)
      queue hworker (TimedJob 1000000)
      threadDelay 500000
      killThread wthread1
      killThread wthread2
      wthread3 <- forkIO (worker hworker)
      threadDelay 10000000
      destroy hworker
      v <- takeMVar mvar
      killThread wthread3
      killThread mthread
      assertEqual "State should be 2, since first 2 failed" 2 v

    it "should work with multiple monitors" $ do
      mvar <- newMVar 0
      hworker <-
        createWith
          (conf "timedworker-3" (TimedState mvar)) { hwconfigTimeout = 5 }
      wthread1 <- forkIO (worker hworker)
      wthread2 <- forkIO (worker hworker)
      -- NOTE(dbp 2015-07-24): This might seem silly, but it
      -- was actually sufficient to expose a race condition.
      mthread1 <- forkIO (monitor hworker)
      mthread2 <- forkIO (monitor hworker)
      mthread3 <- forkIO (monitor hworker)
      mthread4 <- forkIO (monitor hworker)
      mthread5 <- forkIO (monitor hworker)
      mthread6 <- forkIO (monitor hworker)
      queue hworker (TimedJob 1000000)
      queue hworker (TimedJob 1000000)
      threadDelay 500000
      killThread wthread1
      killThread wthread2
      wthread3 <- forkIO (worker hworker)
      threadDelay 30000000
      destroy hworker
      v <- takeMVar mvar
      killThread wthread3
      mapM_ killThread [mthread1, mthread2, mthread3, mthread4, mthread5, mthread6]
      assertEqual "State should be 2, since first 2 failed" 2 v
      -- NOTE(dbp 2015-07-24): It would be really great to have a
      -- test that went after a race between the retry logic and
      -- the monitors (ie, assume that the job completed with
      -- Retry, and it happened to complete right at the timeout
      -- period).  I'm not sure if I could get that sort of
      -- precision without adding other delay mechanisms, or
      -- something to make it more deterministic.

  describe "Scheduled and Recurring Jobs" $ do
    it "should execute job at scheduled time" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "simpleworker-1" (SimpleState mvar))
      wthread <- forkIO (worker hworker)
      mthread <- forkIO (monitor hworker)
      time <- getCurrentTime
      queueScheduled hworker SimpleJob (addUTCTime 1 time)
      queueScheduled hworker SimpleJob (addUTCTime 2 time)
      queueScheduled hworker SimpleJob (addUTCTime 4 time)
      threadDelay 1500000 >> readMVar mvar >>= shouldBe 1
      threadDelay 1000000 >> readMVar mvar >>= shouldBe 2
      threadDelay 1000000 >> readMVar mvar >>= shouldBe 2
      threadDelay 1000000 >> readMVar mvar >>= shouldBe 3
      killThread wthread
      killThread mthread
      destroy hworker

    it "should execute a recurring job" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "recurringworker-1" (RecurringState mvar))
      wthread <- forkIO (worker hworker)
      mthread <- forkIO (monitor hworker)
      time <- getCurrentTime
      queueScheduled hworker RecurringJob (addUTCTime 2 time)
      threadDelay 3000000 >> readMVar mvar >>= shouldBe 1
      threadDelay 2000000 >> readMVar mvar >>= shouldBe 2
      threadDelay 2000000 >> readMVar mvar >>= shouldBe 3
      threadDelay 2000000 >> readMVar mvar >>= shouldBe 4
      destroy hworker
      killThread wthread
      killThread mthread

  describe "Broken jobs" $
    it "should store broken jobs" $ do
      -- NOTE(dbp 2015-08-09): The more common way this could
      -- happen is that you change your serialization format. But
      -- we can abuse this by creating two different workers
      -- pointing to the same queue, and submit jobs in one, try
      -- to run them in another, where the types are different.
      mvar <- newMVar 0
      hworker1 <- createWith (conf "broken-1" (TimedState mvar)) { hwconfigTimeout = 5 }
      hworker2 <- createWith (conf "broken-1" (SimpleState mvar)) { hwconfigTimeout = 5 }
      wthread <- forkIO (worker hworker1)
      queue hworker2 SimpleJob
      threadDelay 100000
      brokenJobs <- broken hworker2
      killThread wthread
      destroy hworker1
      v <- takeMVar mvar
      assertEqual "State should be 0, as nothing should have happened" 0 v
      assertEqual "Should be one broken job, as serialization is wrong" 1 (length brokenJobs)

  describe "Dump jobs" $ do
    it "should return the job that was queued" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "dump-1" (SimpleState mvar)) { hwconfigTimeout = 5 }
      queue hworker SimpleJob
      res <- jobs hworker
      destroy hworker
      assertEqual "Should be [SimpleJob]" [SimpleJob] res

    it "should return jobs in order (most recently added at front; worker pulls from back)" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "dump-2" (TimedState mvar)) { hwconfigTimeout = 5 }
      queue hworker (TimedJob 1)
      queue hworker (TimedJob 2)
      res <- jobs hworker
      destroy hworker
      assertEqual "Should by [TimedJob 2, TimedJob 1]" [TimedJob 2, TimedJob 1] res

  describe "Large jobs" $ do
    it "should be able to deal with lots of large jobs" $ do
      mvar <- newMVar 0
      hworker <- createWith (conf "big-1" (BigState mvar))
      wthread1 <- forkIO (worker hworker)
      wthread2 <- forkIO (worker hworker)
      wthread3 <- forkIO (worker hworker)
      wthread4 <- forkIO (worker hworker)
      let content = T.intercalate "\n" (take 1000 (repeat "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"))
      replicateM_ 5000 (queue hworker (BigJob content))
      threadDelay 10000000
      killThread wthread1
      killThread wthread2
      killThread wthread3
      killThread wthread4
      destroy hworker
      v <- takeMVar mvar
      assertEqual "Should have processed 5000" 5000 v


data SimpleJob =
  SimpleJob deriving (Generic, Show, Eq)

instance ToJSON SimpleJob
instance FromJSON SimpleJob

newtype SimpleState =
  SimpleState (MVar Int)

instance Job SimpleState SimpleJob where
  job Hworker { hworkerState = SimpleState mvar } SimpleJob =
    modifyMVarMasked_ mvar (return . (+1)) >> return Success


data ExJob =
  ExJob deriving (Generic, Show)

instance ToJSON ExJob
instance FromJSON ExJob

newtype ExState =
  ExState (MVar Int)

instance Job ExState ExJob where
  job Hworker { hworkerState = ExState mvar } ExJob = do
    modifyMVarMasked_ mvar (return . (+1))
    v <- readMVar mvar
    if v > 1
      then return Success
      else error "ExJob: failing badly!"


data RetryJob =
  RetryJob deriving (Generic, Show)

instance ToJSON RetryJob
instance FromJSON RetryJob

newtype RetryState =
  RetryState (MVar Int)

instance Job RetryState RetryJob where
  job Hworker { hworkerState = RetryState mvar } RetryJob = do
    modifyMVarMasked_ mvar (return . (+1))
    v <- readMVar mvar
    if v > 1
      then return Success
      else return (Retry "RetryJob retries")


data FailJob =
  FailJob deriving (Eq, Generic, Show)

instance ToJSON FailJob
instance FromJSON FailJob

newtype FailState =
  FailState (MVar Int)

instance Job FailState FailJob where
  job Hworker { hworkerState = FailState mvar } FailJob = do
    modifyMVarMasked_ mvar (return . (+1))
    v <- readMVar mvar
    if v > 1
      then return Success
      else return (Failure "FailJob fails")


data AlwaysFailJob =
  AlwaysFailJob deriving (Eq, Generic, Show)

instance ToJSON AlwaysFailJob
instance FromJSON AlwaysFailJob

newtype AlwaysFailState =
  AlwaysFailState (MVar Int)

instance Job AlwaysFailState AlwaysFailJob where
  job Hworker { hworkerState = AlwaysFailState mvar} AlwaysFailJob = do
    modifyMVarMasked_ mvar (return . (+1))
    return (Failure "AlwaysFailJob fails")


data TimedJob =
  TimedJob Int deriving (Generic, Show, Eq)

instance ToJSON TimedJob
instance FromJSON TimedJob

newtype TimedState =
  TimedState (MVar Int)

instance Job TimedState TimedJob where
  job Hworker { hworkerState = TimedState mvar } (TimedJob delay) = do
    threadDelay delay
    modifyMVarMasked_ mvar (return . (+1))
    return Success


data BigJob =
  BigJob Text deriving (Generic, Show, Eq)

instance ToJSON BigJob
instance FromJSON BigJob

newtype BigState =
  BigState (MVar Int)

instance Job BigState BigJob where
  job Hworker { hworkerState = BigState mvar } (BigJob _) =
    modifyMVarMasked_ mvar (return . (+1)) >> return Success


data RecurringJob =
  RecurringJob deriving (Generic, Show, Eq)

instance ToJSON RecurringJob
instance FromJSON RecurringJob

newtype RecurringState =
  RecurringState (MVar Int)

instance Job RecurringState RecurringJob where
  job hw@Hworker{ hworkerState = RecurringState mvar} RecurringJob = do
    modifyMVarMasked_ mvar (return . (+1))
    time <- getCurrentTime
    queueScheduled hw RecurringJob (addUTCTime 1.99 time)
    return Success


conf :: Text -> s -> HworkerConfig s
conf n s =
  (defaultHworkerConfig n s)
    { hwconfigLogger = const (return ())
    , hwconfigExceptionBehavior = FailOnException
    , hwconfigTimeout = 4
    }


startBatch :: Hworker s t -> Maybe Integer -> IO BatchId
startBatch hw expiration =
  initBatch hw expiration >>=
    \case
      Just batch -> return batch
      Nothing    -> fail "Failed to create batch"


expectBatchSummary :: Hworker s t -> BatchId -> IO BatchSummary
expectBatchSummary hw batch =
  batchSummary hw batch >>=
    \case
      Just summary -> return summary
      Nothing      -> fail "Failed to getch batch summary"
