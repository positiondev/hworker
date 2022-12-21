{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE FunctionalDependencies     #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StrictData                 #-}

{-|

This module contains an at-least-once persistent job processing queue
backed by Redis. It depends upon Redis not losing data once it has
acknowledged it, and guaranteeing the atomicity that is specified for
commands like EVAL (ie, that if you do several things within an EVAL,
they will all happen or none will happen). Nothing has been tested
with Redis clusters (and it likely will not work).

An example use is the following (see the repository for a
slightly expanded version; also, the test cases in the repository are
also good examples):


> data PrintJob = Print deriving (Generic, Show)
> data State = State (MVar Int)
> instance ToJSON PrintJob
> instance FromJSON PrintJob
>
> instance Job State PrintJob where
>   job (State mvar) Print =
>     do v <- takeMVar mvar
>        putMVar mvar (v + 1)
>        putStrLn $ "A(" ++ show v ++ ")"
>        return Success
>
> main = do mvar <- newMVar 0
>           hworker <- create "printer" (State mvar)
>           forkIO (worker hworker)
>           forkIO (monitor hworker)
>           forkIO (forever $ queue hworker Print >> threadDelay 1000000)
>           forever (threadDelay 1000000)


-}

module System.Hworker
  ( -- * Types
    Result(..)
  , Job(..)
  , Hworker(..)
  , HworkerConfig(..)
  , defaultHworkerConfig
  , ExceptionBehavior(..)
  , RedisConnection(..)
  , BatchId(..)
  , BatchStatus(..)
  , BatchSummary(..)
    -- * Managing Workers
  , create
  , createWith
  , destroy
  , batchSummary
  , worker
  , monitor
    -- * Queuing Jobs
  , queue
  , queueBatch
  , streamBatch
  , initBatch
  , stopBatchQueueing
    -- * Inspecting Workers
  , jobs
  , failed
  , broken
    -- * Debugging Utilities
  , debugger
  , batchCounter
  ) where

--------------------------------------------------------------------------------
import           Control.Arrow           ( second)
import           Control.Concurrent      ( threadDelay)
import           Control.Exception       ( SomeException
                                         , catch
                                         , catchJust
                                         , asyncExceptionFromException
                                         , AsyncException
                                         )
import           Control.Monad           ( forM_, forever, void, when )
import           Control.Monad.Trans     ( liftIO, lift )
import           Data.Aeson              ( FromJSON, ToJSON, (.=), (.:) )
import qualified Data.Aeson             as A
import           Data.ByteString         ( ByteString )
import qualified Data.ByteString.Char8  as B8
import qualified Data.ByteString.Lazy   as LB
import           Data.Conduit            ( ConduitT )
import qualified Data.Conduit           as Conduit
import           Data.Either             ( isRight )
import           Data.Maybe              ( isJust, mapMaybe, listToMaybe )
import           Data.Text               ( Text )
import qualified Data.Text              as T
import qualified Data.Text.Encoding     as T
import           Data.Time.Clock         ( NominalDiffTime
                                         , UTCTime (..)
                                         , diffUTCTime
                                         , getCurrentTime
                                         )
import           Data.UUID               ( UUID )
import qualified Data.UUID              as UUID
import qualified Data.UUID.V4           as UUID
import           Database.Redis          ( Redis
                                         , RedisTx
                                         , TxResult(..)
                                         , Connection
                                         , ConnectInfo
                                         , runRedis
                                         )
import qualified Database.Redis         as R
import           GHC.Generics            ( Generic )
--------------------------------------------------------------------------------



-- | Jobs can return 'Success', 'Retry' (with a message), or 'Failure'
-- (with a message). Jobs that return 'Failure' are stored in the
-- 'failed' queue and are not re-run. Jobs that return 'Retry' are re-run.

data Result
  = Success
  | Retry Text
  | Failure Text
  deriving (Generic, Show)


instance ToJSON Result
instance FromJSON Result


-- | Each Worker that you create will be responsible for one type of
-- job, defined by a 'Job' instance.
--
-- The job can do many different things (as the value can be a
-- variant), but be careful not to break deserialization if you add
-- new things it can do.
--
-- The job will take some state (passed as the `s` parameter), which
-- does not vary based on the job, and the actual job data
-- structure. The data structure (the `t` parameter) will be stored
-- and copied a few times in Redis while in the lifecycle, so
-- generally it is a good idea for it to be relatively small (and have
-- it be able to look up data that it needs while the job is running).
--
-- Finally, while deriving FromJSON and ToJSON instances automatically
-- might seem like a good idea, you will most likely be better off
-- defining them manually, so you can make sure they are backwards
-- compatible if you change them, as any jobs that can't be
-- deserialized will not be run (and will end up in the 'broken'
-- queue). This will only happen if the queue is non-empty when you
-- replace the running application version, but this is obviously
-- possible and could be likely depending on your use.

class (FromJSON t, ToJSON t, Show t) => Job s t | s -> t where
  job :: s -> t -> IO Result


-- | What should happen when an unexpected exception is thrown in a
-- job - it can be treated as either a 'Failure' (the default) or a
-- 'Retry' (if you know the only exceptions are triggered by
-- intermittent problems).

data ExceptionBehavior
  = RetryOnException
  | FailOnException


type JobId = Text


-- | A unique identifier for grouping jobs together.

newtype BatchId =
  BatchId UUID
  deriving (ToJSON, FromJSON, Eq, Show)


-- | Represents the current status of a batch. A batch is considered to be
-- "queueing" if jobs can still be added to the batch. While jobs are
-- queueing it is possible for them to be "processing" during that time.
-- The status only changes to "processing" once jobs can no longer be queued
-- but are still being processed. The batch is then finished once all jobs
-- are processed (they have either failed or succeeded).

data BatchStatus
  = BatchQueueing
  | BatchProcessing
  | BatchFinished
  deriving (Eq, Show)


-- |  A summary of a particular batch, including figures on the total number
-- of jobs queued, the number of jobs that have completed (i.e.
-- failed or succeeded), the number of jobs succeeded, the number of jobs
-- failed, the number of jobs retried, and the current status of the
-- batch overall.

data BatchSummary =
  BatchSummary
    { batchSummaryID        :: BatchId
    , batchSummaryQueued    :: Int
    , batchSummaryCompleted :: Int
    , batchSummarySuccesses :: Int
    , batchSummaryFailures  :: Int
    , batchSummaryRetries   :: Int
    , batchSummaryStatus    :: BatchStatus
    } deriving (Eq, Show)


data JobRef =
  JobRef JobId (Maybe BatchId)
  deriving (Eq, Show)


instance ToJSON JobRef where
  toJSON (JobRef j b) = A.object ["j" .= j, "b" .= b]


instance FromJSON JobRef where
  -- NOTE(rjbf 2022-11-19): This is just here for the sake of migration and
  -- can be removed eventually. Before `JobRef`, which is encoded as
  -- a JSON object, there was a just a `String` representing the job ID.

  parseJSON (A.String j) = pure (JobRef j Nothing)
  parseJSON val = A.withObject "JobRef" (\o -> JobRef <$> o .: "j" <*> o .: "b") val


hwlog :: Show a => Hworker s t -> a -> IO ()
hwlog hw a =
  hworkerLogger hw (hworkerName hw, a)


-- | The worker data type - it is parametrized be the worker
-- state (the `s`) and the job type (the `t`).

data Hworker s t =
  Hworker
    { hworkerName              :: ByteString
    , hworkerState             :: s
    , hworkerConnection        :: Connection
    , hworkerExceptionBehavior :: ExceptionBehavior
    , hworkerLogger            :: forall a. Show a => a -> IO ()
    , hworkerJobTimeout        :: NominalDiffTime
    , hworkerFailedQueueSize   :: Int
    , hworkerDebug             :: Bool
    , hworkerBatchCompleted    :: BatchSummary -> IO ()
    }


-- | When configuring a worker, you can tell it to use an existing
-- redis connection pool (which you may have for the rest of your
-- application). Otherwise, you can specify connection info. By
-- default, hworker tries to connect to localhost, which may not be
-- true for your production application.

data RedisConnection
  = RedisConnectInfo ConnectInfo
  | RedisConnection Connection


-- | The main configuration for workers.
--
-- Each pool of workers should have a unique `hwconfigName`, as the
-- queues are set up by that name, and if you have different types of
-- data written in, they will likely be unable to be deserialized (and
-- thus could end up in the 'broken' queue).
--
-- The 'hwconfigLogger' defaults to writing to stdout, so you will
-- likely want to replace that with something appropriate (like from a
-- logging package).
--
-- The `hwconfigTimeout` is really important. It determines the length
-- of time after a job is started before the 'monitor' will decide
-- that the job must have died and will restart it. If it is shorter
-- than the length of time that a normal job takes to complete, the
-- jobs _will_ be run multiple times. This is _semantically_ okay, as
-- this is an at-least-once processor, but obviously won't be
-- desirable. It defaults to 120 seconds.
--
-- The 'hwconfigExceptionBehavior' controls what happens when an
-- exception is thrown within a job.
--
-- 'hwconfigFailedQueueSize' controls how many 'failed' jobs will be
-- kept. It defaults to 1000.

data HworkerConfig s =
  HworkerConfig
    { hwconfigName              :: Text
    , hwconfigState             :: s
    , hwconfigRedisConnectInfo  :: RedisConnection
    , hwconfigExceptionBehavior :: ExceptionBehavior
    , hwconfigLogger            :: forall a. Show a => a -> IO ()
    , hwconfigTimeout           :: NominalDiffTime
    , hwconfigFailedQueueSize   :: Int
    , hwconfigDebug             :: Bool
    , hwconfigBatchCompleted    :: BatchSummary -> IO ()
    }


-- | The default worker config - it needs a name and a state (as those
-- will always be unique).

defaultHworkerConfig :: Text -> s -> HworkerConfig s
defaultHworkerConfig name state =
  HworkerConfig
    { hwconfigName              = name
    , hwconfigState             = state
    , hwconfigRedisConnectInfo  = RedisConnectInfo R.defaultConnectInfo
    , hwconfigExceptionBehavior = FailOnException
    , hwconfigLogger            = print
    , hwconfigTimeout           = 120
    , hwconfigFailedQueueSize   = 1000
    , hwconfigDebug             = False
    , hwconfigBatchCompleted    = const (return ())
    }


-- | Create a new worker with the default 'HworkerConfig'.
--
-- Note that you must create at least one 'worker' and 'monitor' for
-- the queue to actually process jobs (and for it to retry ones that
-- time-out).

create :: Job s t => Text -> s -> IO (Hworker s t)
create name state =
  createWith (defaultHworkerConfig name state)


-- | Create a new worker with a specified 'HworkerConfig'.
--
-- Note that you must create at least one 'worker' and 'monitor' for
-- the queue to actually process jobs (and for it to retry ones that
-- time-out).

createWith :: Job s t => HworkerConfig s -> IO (Hworker s t)
createWith HworkerConfig{..} = do
  conn <-
    case hwconfigRedisConnectInfo of
      RedisConnectInfo c -> R.connect c
      RedisConnection  c -> return c

  return $
    Hworker
      { hworkerName              = T.encodeUtf8 hwconfigName
      , hworkerState             = hwconfigState
      , hworkerConnection        = conn
      , hworkerExceptionBehavior = hwconfigExceptionBehavior
      , hworkerLogger            = hwconfigLogger
      , hworkerJobTimeout        = hwconfigTimeout
      , hworkerFailedQueueSize   = hwconfigFailedQueueSize
      , hworkerDebug             = hwconfigDebug
      , hworkerBatchCompleted    = hwconfigBatchCompleted
      }


-- | Destroy a worker. This will delete all the queues, clearing out
-- all existing 'jobs', the 'broken' and 'failed' queues, and the hashes for
-- batched jobs. There is no need to do this in normal applications
-- (and most likely, you won't want to).

destroy :: Job s t => Hworker s t -> IO ()
destroy hw =
  let
    batchKeys =
      "hworker-batch-" <> hworkerName hw <> "*"
  in
  void $ runRedis (hworkerConnection hw) $ do
    R.keys batchKeys >>=
      \case
        Left  err  -> liftIO $ hwlog hw err
        Right keys -> void $ R.del keys

    R.del
      [ jobQueue hw
      , progressQueue hw
      , brokenQueue hw
      , failedQueue hw
      ]


jobQueue :: Hworker s t -> ByteString
jobQueue hw =
  "hworker-jobs-" <> hworkerName hw


progressQueue :: Hworker s t -> ByteString
progressQueue hw =
  "hworker-progress-" <> hworkerName hw


brokenQueue :: Hworker s t -> ByteString
brokenQueue hw =
  "hworker-broken-" <> hworkerName hw


failedQueue :: Hworker s t -> ByteString
failedQueue hw =
  "hworker-failed-" <> hworkerName hw


batchCounter :: Hworker s t -> BatchId -> ByteString
batchCounter hw (BatchId batch) =
  "hworker-batch-" <> hworkerName hw <> ":" <> UUID.toASCIIBytes batch


-- | Adds a job to the queue. Returns whether the operation succeeded.

queue :: Job s t => Hworker s t -> t -> IO Bool
queue hw j = do
  jobId <- UUID.toText <$> UUID.nextRandom
  result <-
    runRedis (hworkerConnection hw)
      $ R.lpush (jobQueue hw) [LB.toStrict $ A.encode (JobRef jobId Nothing, j)]
  return $ isRight result


-- | Adds jobs to the queue, but as part of a particular batch of jobs.
-- It takes the `BatchId` of the specified job, a `Bool` that when `True`
-- closes the batch to further queueing, and a list of jobs to be queued, and
-- returns whether the operation succeeded. The process is atomic
-- so that if a single job fails to queue then then none of the jobs
-- in the list will queue.

queueBatch :: Job s t => Hworker s t -> BatchId -> Bool -> [t] -> IO Bool
queueBatch hw batch close js =
  withBatchQueue hw batch $ runRedis (hworkerConnection hw) $
    R.multiExec $ do
      mapM_
        ( \j -> do
            jobId <- UUID.toText <$> liftIO UUID.nextRandom
            let ref = JobRef jobId (Just batch)
            _ <- R.lpush (jobQueue hw) [LB.toStrict $ A.encode (ref, j)]

            -- Do the counting outside of the transaction, hence runRedis here.
            liftIO
              $ runRedis (hworkerConnection hw)
              $ R.hincrby (batchCounter hw batch) "queued" 1
        )
        js

      when close
        $ void
        $ R.hset (batchCounter hw batch) "status" "processing"

      return (pure ())


-- | Like 'queueBatch', but instead of a list of jobs, it takes a conduit
-- that streams jobs in.

streamBatch ::
  Job s t =>
  Hworker s t -> BatchId -> Bool -> ConduitT () t RedisTx () -> IO Bool
streamBatch hw batch close producer =
  let
    sink =
      Conduit.await >>=
        \case
          Nothing -> do
            when close
              $ void . lift
              $ R.hset (batchCounter hw batch) "status" "processing"
            return (pure ())

          Just j -> do
            jobId <- UUID.toText <$> liftIO UUID.nextRandom
            let ref = JobRef jobId (Just batch)
            _ <- lift $ R.lpush (jobQueue hw) [LB.toStrict $ A.encode (ref, j)]

            -- Do the counting outside of the transaction, hence runRedis here.
            _ <-
              liftIO
                $ runRedis (hworkerConnection hw)
                $ R.hincrby (batchCounter hw batch) "queued" 1

            sink
  in
  withBatchQueue hw batch
    $ runRedis (hworkerConnection hw)
    $ R.multiExec (Conduit.connect producer sink)


withBatchQueue ::
  Job s t => Hworker s t -> BatchId -> IO (TxResult ()) -> IO Bool
withBatchQueue hw batch process =
  runRedis (hworkerConnection hw) (batchSummary' hw batch) >>=
    \case
      Nothing -> do
        hwlog hw $ "BATCH NOT FOUND: " <> show batch
        return False

      Just summary | batchSummaryStatus summary == BatchQueueing ->
        catch
          ( process >>=
              \case
                TxSuccess () ->
                  return True

                TxAborted -> do
                  hwlog hw ("TRANSACTION ABORTED" :: String)
                  runRedis (hworkerConnection hw) $ resetBatchSummary hw summary
                  return False

                TxError err -> do
                  hwlog hw err
                  runRedis (hworkerConnection hw) $ resetBatchSummary hw summary
                  return False
          )
          ( \(e :: SomeException) -> do
              hwlog hw $ show e
              runRedis (hworkerConnection hw) (resetBatchSummary hw summary)
              return False
          )

      Just _-> do
        hwlog hw $
          mconcat
            [ "QUEUEING COMPLETED FOR BATCH: "
            , show batch
            , ". CANNOT ENQUEUE NEW JOBS."
            ]
        return False


-- | Prevents queueing new jobs to a batch. If the number of jobs completed equals
-- the number of jobs queued, then the status of the batch is immediately set
-- to `BatchFinished`, otherwise it's set to `BatchProcessing`.

stopBatchQueueing :: Hworker s t -> BatchId -> IO ()
stopBatchQueueing hw batch =
  runRedis (hworkerConnection hw) $ do
    batchSummary' hw batch >>=
      \case
        Nothing ->
          liftIO $ hwlog hw $ "Batch not found: " <> show batch

        Just summary | batchSummaryCompleted summary >= batchSummaryQueued summary ->
          void
            $ R.hset (batchCounter hw batch) "status"
            $ encodeBatchStatus BatchFinished

        Just _->
          void
            $ R.hset (batchCounter hw batch) "status"
            $ encodeBatchStatus BatchProcessing


-- | Creates a new worker thread. This is blocking, so you will want to
-- 'forkIO' this into a thread. You can have any number of these (and
-- on any number of servers); the more there are, the faster jobs will
-- be processed.

worker :: Job s t => Hworker s t -> IO ()
worker hw =
  let
    delayAndRun =
      threadDelay 10000 >> worker hw

    justRun =
      worker hw

    runJob action = do
      eitherResult <-
        catchJust
          ( \(e :: SomeException) ->
              if isJust (asyncExceptionFromException e :: Maybe AsyncException)
                then Nothing
                else Just e
          )
          ( Right <$> action )
          ( return . Left )

      case eitherResult of
        Left exception ->
          let
            resultMessage =
              case hworkerExceptionBehavior hw of
                RetryOnException -> Retry
                FailOnException  -> Failure
          in
          return
            $ resultMessage
            $ "Exception raised: " <> (T.pack . show) exception

        Right result ->
          return result
  in do
  now <- getCurrentTime

  eitherReply <-
    runRedis (hworkerConnection hw) $
      R.eval
        "local job = redis.call('rpop',KEYS[1])\n\
        \if job ~= nil then\n\
        \  redis.call('hset', KEYS[2], tostring(job), ARGV[1])\n\
        \  return job\n\
        \else\n\
        \  return nil\n\
        \end"
        [jobQueue hw, progressQueue hw]
        [LB.toStrict $ A.encode now]

  case eitherReply of
    Left err ->
      hwlog hw err >> delayAndRun

    Right Nothing ->
      delayAndRun

    Right (Just t) -> do
      when (hworkerDebug hw) $  hwlog hw ("WORKER RUNNING" :: Text, t)

      case A.decodeStrict t of
        Nothing -> do
          hwlog hw ("BROKEN JOB" :: Text, t)
          now' <- getCurrentTime

          withNil hw $
            R.eval
              "local del = redis.call('hdel', KEYS[1], ARGV[1])\n\
              \if del == 1 then\n\
              \  redis.call('hset', KEYS[2], ARGV[1], ARGV[2])\n\
              \end\n\
              \return nil"
              [progressQueue hw, brokenQueue hw]
              [t, LB.toStrict $ A.encode now']

          delayAndRun

        Just (JobRef _ maybeBatch, j) -> do
          runJob (job (hworkerState hw) j) >>=
            \case
              Success -> do
                when (hworkerDebug hw) $ hwlog hw ("JOB COMPLETE" :: Text, t)

                case maybeBatch of
                  Nothing -> do
                    deletionResult <-
                      runRedis (hworkerConnection hw)
                        $ R.hdel (progressQueue hw) [t]

                    case deletionResult of
                      Left err -> hwlog hw err >> delayAndRun
                      Right 1  -> justRun
                      Right n  -> do
                        hwlog hw ("Job done: did not delete 1, deleted " <> show n)
                        delayAndRun

                  Just batch ->
                    withMaybe hw
                      ( R.eval
                          "local del = redis.call('hdel', KEYS[1], ARGV[1])\n\
                          \if del == 1 then\n\
                          \  local batch = KEYS[2]\n\
                          \  redis.call('hincrby', batch, 'successes', '1')\n\
                          \  local completed = redis.call('hincrby', batch, 'completed', '1')\n\
                          \  local queued = redis.call('hincrby', batch, 'queued', '0')\n\
                          \  local status = redis.call('hget', batch, 'status')\n\
                          \  if tonumber(completed) >= tonumber(queued) and status == 'processing' then\n\
                          \    redis.call('hset', batch, 'status', 'finished')\n\
                          \  end\n\
                          \  return redis.call('hgetall', batch)\
                          \end\n\
                          \return nil"
                          [progressQueue hw, batchCounter hw batch]
                          [t]
                      )
                      ( \hm ->
                          case decodeBatchSummary batch hm of
                            Nothing -> do
                              hwlog hw ("Job done: did not delete 1" :: Text)
                              delayAndRun

                            Just summary -> do
                              when (batchSummaryStatus summary == BatchFinished)
                                $ hworkerBatchCompleted hw summary
                              justRun
                      )


              Retry msg -> do
                hwlog hw ("RETRY: " <> msg)

                case maybeBatch of
                  Nothing -> do
                    withNil hw $
                      R.eval
                        "local del = redis.call('hdel', KEYS[1], ARGV[1])\n\
                        \if del == 1 then\n\
                        \  redis.call('lpush', KEYS[2], ARGV[1])\n\
                        \end\n\
                        \return nil"
                        [progressQueue hw, jobQueue hw]
                        [t]

                    delayAndRun

                  Just batch -> do
                    withNil hw $
                      R.eval
                        "local del = redis.call('hdel', KEYS[1], ARGV[1])\n\
                        \if del == 1 then\n\
                        \  redis.call('lpush', KEYS[2], ARGV[1])\n\
                        \  redis.call('hincrby', KEYS[3], 'retries', '1')\n\
                        \end\n\
                        \return nil"
                        [progressQueue hw, jobQueue hw, batchCounter hw batch]
                        [t]

                    delayAndRun

              Failure msg -> do
                hwlog hw ("Failure: " <> msg)

                case maybeBatch of
                  Nothing -> do
                    withNil hw $
                      R.eval
                        "local del = redis.call('hdel', KEYS[1], ARGV[1])\n\
                        \if del == 1 then\n\
                        \  redis.call('lpush', KEYS[2], ARGV[1])\n\
                        \  redis.call('ltrim', KEYS[2], 0, ARGV[2])\n\
                        \end\n\
                        \return nil"
                        [progressQueue hw, failedQueue hw]
                        [t, B8.pack (show (hworkerFailedQueueSize hw - 1))]

                    delayAndRun

                  Just batch -> do
                    withMaybe hw
                      ( R.eval
                          "local del = redis.call('hdel', KEYS[1], ARGV[1])\n\
                          \if del == 1 then\n\
                          \  redis.call('lpush', KEYS[2], ARGV[1])\n\
                          \  redis.call('ltrim', KEYS[2], 0, ARGV[2])\n\
                          \  local batch = KEYS[3]\n\
                          \  redis.call('hincrby', batch, 'failures', '1')\n\
                          \  local completed = redis.call('hincrby', batch, 'completed', '1')\n\
                          \  local queued = redis.call('hincrby', batch, 'queued', '0')\n\
                          \  local status = redis.call('hget', batch, 'status')\n\
                          \  if tonumber(completed) >= tonumber(queued) and status == 'processing' then\n\
                          \    redis.call('hset', batch, 'status', 'finished')\n\
                          \    return redis.call('hgetall', batch)\
                          \  end\n\
                          \end\n\
                          \return nil"
                          [progressQueue hw, failedQueue hw, batchCounter hw batch]
                          [t, B8.pack (show (hworkerFailedQueueSize hw - 1))]
                      )
                      ( \hm ->
                          forM_ (decodeBatchSummary batch hm)
                            $ hworkerBatchCompleted hw
                      )
                    delayAndRun


-- | Start a monitor. Like 'worker', this is blocking, so should be
-- started in a thread. This is responsible for retrying jobs that
-- time out (which can happen if the processing thread is killed, for
-- example). You need to have at least one of these running to have
-- the retry happen, but it is safe to have any number running.

monitor :: Job s t => Hworker s t -> IO ()
monitor hw =
  forever $ do
    now <- getCurrentTime

    withList hw (R.hkeys (progressQueue hw)) $ \js ->
      forM_ js $ \j ->
        withMaybe hw (R.hget (progressQueue hw) j) $
          \start ->
            let
              duration =
                diffUTCTime now (parseTime start)

            in
            when (duration > hworkerJobTimeout hw) $ do
              n <-
                withInt hw $
                  R.eval
                    "local del = redis.call('hdel', KEYS[2], ARGV[1])\n\
                    \if del == 1 then\
                    \  redis.call('rpush', KEYS[1], ARGV[1])\n\
                    \end\n\
                    \return del"
                    [jobQueue hw, progressQueue hw]
                    [j]
              when (hworkerDebug hw)
                $ hwlog hw ("MONITOR RV" :: Text, n)
              when (hworkerDebug hw && n == 1)
                $ hwlog hw ("MONITOR REQUEUED" :: Text, j)

    -- NOTE(dbp 2015-07-25): We check every 1/10th of timeout.
    threadDelay (floor $ 100000 * hworkerJobTimeout hw)


-- | Returns the jobs that could not be deserialized, most likely
-- because you changed the 'ToJSON'/'FromJSON' instances for you job
-- in a way that resulted in old jobs not being able to be converted
-- back from json. Another reason for jobs to end up here (and much
-- worse) is if you point two instances of 'Hworker', with different
-- job types, at the same queue (i.e., you re-use the name). Then
-- anytime a worker from one queue gets a job from the other it would
-- think it is broken.

broken :: Hworker s t -> IO [(ByteString, UTCTime)]
broken hw =
  runRedis (hworkerConnection hw) (R.hgetall (brokenQueue hw)) >>=
    \case
      Left err -> hwlog hw err >> return []
      Right xs -> return (map (second parseTime) xs)


jobsFromQueue :: Job s t => Hworker s t -> ByteString -> IO [t]
jobsFromQueue hw q =
  runRedis (hworkerConnection hw) (R.lrange q 0 (-1)) >>=
    \case
      Left err ->
        hwlog hw err >> return []

      Right [] ->
        return []

      Right xs ->
        return $ mapMaybe (fmap (\(JobRef _ _, x) -> x) . A.decodeStrict) xs


-- | Returns all pending jobs.

jobs :: Job s t => Hworker s t -> IO [t]
jobs hw =
  jobsFromQueue hw (jobQueue hw)


-- | Returns all failed jobs. This is capped at the most recent
-- 'hworkerconfigFailedQueueSize' jobs that returned 'Failure' (or
-- threw an exception when 'hworkerconfigExceptionBehavior' is
-- 'FailOnException').

failed :: Job s t => Hworker s t -> IO [t]
failed hw =
  jobsFromQueue hw (failedQueue hw)


-- | Logs the contents of the jobqueue and the inprogress queue at
-- `microseconds` intervals.

debugger :: Job s t => Int -> Hworker s t -> IO ()
debugger microseconds hw =
  forever $ do
    withList hw (R.hkeys (progressQueue hw)) $
      \running ->
        withList hw (R.lrange (jobQueue hw) 0 (-1))
          $ \queued -> hwlog hw ("DEBUG" :: Text, queued, running)

    threadDelay microseconds


-- | Initializes a batch of jobs. By default the information for tracking a
-- batch of jobs, created by this function, will expires a week from
-- its creation. The optional `seconds` argument can be used to override this.

initBatch :: Hworker s t -> Maybe Integer -> IO (Maybe BatchId)
initBatch hw mseconds = do
  batch <- BatchId <$> UUID.nextRandom
  runRedis (hworkerConnection hw) $ do
    r <-
      R.hmset (batchCounter hw batch)
        [ ("queued", "0")
        , ("completed", "0")
        , ("successes", "0")
        , ("failures", "0")
        , ("retries", "0")
        , ("status", "queueing")
        ]
    case r of
      Left err ->
        liftIO (hwlog hw err) >> return Nothing

      Right _ -> do
        case mseconds of
          Nothing -> return ()
          Just s  -> void $ R.expire (batchCounter hw batch) s

        return (Just batch)


-- | Return a summary of the batch.

batchSummary :: Hworker s t -> BatchId -> IO (Maybe BatchSummary)
batchSummary hw batch =
  runRedis (hworkerConnection hw) (batchSummary' hw batch)


batchSummary' :: Hworker s t -> BatchId -> Redis (Maybe BatchSummary)
batchSummary' hw batch = do
  R.hgetall (batchCounter hw batch) >>=
    \case
      Left err -> liftIO (hwlog hw err) >> return Nothing
      Right hm -> return $ decodeBatchSummary batch hm


-- Redis helpers follow

withList ::
  Show a => Hworker s t -> Redis (Either a [b]) -> ([b] -> IO ()) -> IO ()
withList hw a f =
  runRedis (hworkerConnection hw) a >>=
    \case
      Left err -> hwlog hw err
      Right [] -> return ()
      Right xs -> f xs


withMaybe ::
  Show a => Hworker s t -> Redis (Either a (Maybe b)) -> (b -> IO ()) -> IO ()
withMaybe hw a f = do
  runRedis (hworkerConnection hw) a >>=
    \case
      Left err       -> hwlog hw err
      Right Nothing  -> return ()
      Right (Just v) -> f v


withNil :: Show a => Hworker s t -> Redis (Either a (Maybe ByteString)) -> IO ()
withNil hw a =
  runRedis (hworkerConnection hw) a >>=
    \case
      Left err -> hwlog hw err
      Right _  -> return ()


withInt :: Hworker s t -> Redis (Either R.Reply Integer) -> IO Integer
withInt hw a =
  runRedis (hworkerConnection hw) a >>=
    \case
      Left err -> hwlog hw err >> return (-1)
      Right n  -> return n


-- Parsing Helpers

encodeBatchStatus :: BatchStatus -> ByteString
encodeBatchStatus BatchQueueing   = "queueing"
encodeBatchStatus BatchProcessing = "processing"
encodeBatchStatus BatchFinished   = "finished"


decodeBatchStatus :: ByteString -> Maybe BatchStatus
decodeBatchStatus "queueing"   = Just BatchQueueing
decodeBatchStatus "processing" = Just BatchProcessing
decodeBatchStatus "finished"   = Just BatchFinished
decodeBatchStatus _            = Nothing


decodeBatchSummary :: BatchId -> [(ByteString, ByteString)] -> Maybe BatchSummary
decodeBatchSummary batch hm =
  BatchSummary batch
    <$> (lookup "queued" hm >>= readMaybe)
    <*> (lookup "completed" hm >>= readMaybe)
    <*> (lookup "successes" hm >>= readMaybe)
    <*> (lookup "failures" hm >>= readMaybe)
    <*> (lookup "retries" hm >>= readMaybe)
    <*> (lookup "status" hm >>= decodeBatchStatus)


parseTime :: ByteString -> UTCTime
parseTime t =
  case A.decodeStrict t of
    Nothing   -> error ("FAILED TO PARSE TIMESTAMP: " <> B8.unpack t)
    Just time -> time


readMaybe :: Read a => ByteString -> Maybe a
readMaybe =
  fmap fst . listToMaybe . reads . B8.unpack


resetBatchSummary :: R.RedisCtx m n => Hworker s t -> BatchSummary -> m ()
resetBatchSummary hw BatchSummary{..} =
  let
    encode =
      B8.pack . show
  in
  void $
    R.hmset (batchCounter hw batchSummaryID)
      [ ("queued", encode batchSummaryQueued)
      , ("completed", encode batchSummaryCompleted)
      , ("successes", encode batchSummarySuccesses)
      , ("failures", encode batchSummaryFailures)
      , ("retries", encode batchSummaryRetries)
      , ("status", encodeBatchStatus batchSummaryStatus)
      ]
