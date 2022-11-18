{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE FunctionalDependencies     #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}

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
       , Hworker
       , HworkerConfig(..)
       , ExceptionBehavior(..)
       , RedisConnection(..)
       , defaultHworkerConfig
       , BatchID(..)
       , BatchStatus(..)
       , BatchJob(..)
         -- * Managing Workers
       , create
       , createWith
       , destroy
       , batchJob
       , worker
       , monitor
         -- * Queuing Jobs
       , queue
       , queueBatched
       , initBatch
         -- * Inspecting Workers
       , jobs
       , failed
       , broken
         -- * Debugging Utilities
       , debugger
       )
       where

import           Control.Arrow           (second)
import           Control.Concurrent      (forkIO, threadDelay)
import           Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import           Control.Exception       (SomeException, catchJust,
                                          asyncExceptionFromException,
                                          AsyncException)
import           Control.Monad           (forM, forever, void, when)
import           Control.Monad.Trans     (liftIO)
import           Data.Aeson              (FromJSON, ToJSON, (.=), (.:) )
import qualified Data.Aeson              as A
import           Data.Aeson.Helpers
import           Data.ByteString         (ByteString)
import qualified Data.ByteString.Char8   as B8
import qualified Data.ByteString.Lazy    as LB
import           Data.Either             (isRight)
import           Data.Maybe              (fromJust, isJust, mapMaybe, listToMaybe)
import           Data.Monoid             ((<>))
import           Data.Text               (Text)
import qualified Data.Text               as T
import qualified Data.Text.Encoding      as T
import           Data.Time.Calendar      (Day (..))
import           Data.Time.Clock         (NominalDiffTime, UTCTime (..),
                                          diffUTCTime, getCurrentTime)
import           Data.UUID                ( UUID )
import qualified Data.UUID               as UUID
import qualified Data.UUID.V4            as UUID
import qualified Database.Redis          as R
import           GHC.Generics            (Generic)

-- | Jobs can return 'Success', 'Retry' (with a message), or 'Failure'
-- (with a message). Jobs that return 'Failure' are stored in the
-- 'failed' queue and are not re-run. Jobs that return 'Retry' are re-run.
data Result = Success
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
-- it be able to look up data that it needs while the job in running).
--
-- Finally, while deriving FromJSON and ToJSON instances automatically
-- might seem like a good idea, you will most likely be better off
-- defining them manually, so you can make sure they are backwards
-- compatible if you change them, as any jobs that can't be
-- deserialized will not be run (and will end up in the 'broken'
-- queue). This will only happen if the queue is non-empty when you
-- replce the running application version, but this is obviously
-- possible and could be likely depending on your use.
class (FromJSON t, ToJSON t, Show t) => Job s t | s -> t where
  job :: s -> t -> IO Result

data JobData t = JobData UTCTime t

-- | What should happen when an unexpected exception is thrown in a
-- job - it can be treated as either a 'Failure' (the default) or a
-- 'Retry' (if you know the only exceptions are triggered by
-- intermittent problems).
data ExceptionBehavior = RetryOnException | FailOnException

type JobID = Text

-- | A unique identifier for grouping jobs together.
newtype BatchID = BatchID UUID deriving (ToJSON, FromJSON, Eq, Show)

data BatchStatus = BatchQueuing | BatchProcessing | BatchFinished
  deriving (Eq, Show)

encodeBatchStatus :: BatchStatus -> ByteString
encodeBatchStatus BatchQueuing    = "queueing"
encodeBatchStatus BatchProcessing = "processing"
encodeBatchStatus BatchFinished   = "finished"

decodeBatchStatus :: ByteString -> Maybe BatchStatus
decodeBatchStatus "queueing"   = Just BatchQueuing
decodeBatchStatus "processing" = Just BatchProcessing
decodeBatchStatus "finished"   = Just BatchFinished
decodeBatchStatus _            = Nothing

data BatchJob =
  BatchJob
    { batchID        :: BatchID
    , batchTotal     :: Int
    , batchCompleted :: Int
    , batchSuccesses :: Int
    , batchFailures  :: Int
    , batchRetries   :: Int
    , batchStatus    :: BatchStatus
    }

data JobRef = JobRef JobID (Maybe BatchID) deriving (Eq, Show)

instance ToJSON JobRef where
  toJSON (JobRef j b) = A.object ["j" .= j, "b" .= b]

instance FromJSON JobRef where
  parseJSON (A.String j) = pure (JobRef j Nothing)
  parseJSON val = A.withObject "JobRef" (\o -> JobRef <$> o .: "j" <*> o .: "b") val

hwlog :: Show a => Hworker s t -> a -> IO ()
hwlog hw a = hworkerLogger hw (hworkerName hw, a)

-- | The worker data type - it is parametrized be the worker
-- state (the `s`) and the job type (the `t`).
data Hworker s t =
     Hworker { hworkerName              :: ByteString
             , hworkerState             :: s
             , hworkerConnection        :: R.Connection
             , hworkerExceptionBehavior :: ExceptionBehavior
             , hworkerLogger            :: forall a. Show a => a -> IO ()
             , hworkerJobTimeout        :: NominalDiffTime
             , hworkerFailedQueueSize   :: Int
             , hworkerDebug             :: Bool
             }

-- | When configuring a worker, you can tell it to use an existing
-- redis connection pool (which you may have for the rest of your
-- application). Otherwise, you can specify connection info. By
-- default, hworker tries to connect to localhost, which may not be
-- true for your production application.
data RedisConnection = RedisConnectInfo R.ConnectInfo
                     | RedisConnection R.Connection

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
     HworkerConfig {
         hwconfigName              :: Text
       , hwconfigState             :: s
       , hwconfigRedisConnectInfo  :: RedisConnection
       , hwconfigExceptionBehavior :: ExceptionBehavior
       , hwconfigLogger            :: forall a. Show a => a -> IO ()
       , hwconfigTimeout           :: NominalDiffTime
       , hwconfigFailedQueueSize   :: Int
       , hwconfigDebug             :: Bool
       }

-- | The default worker config - it needs a name and a state (as those
-- will always be unique).
defaultHworkerConfig :: Text -> s -> HworkerConfig s
defaultHworkerConfig name state =
  HworkerConfig name
                state
                (RedisConnectInfo R.defaultConnectInfo)
                FailOnException
                print
                120
                1000
                False

-- | Create a new worker with the default 'HworkerConfig'.
--
-- Note that you must create at least one 'worker' and 'monitor' for
-- the queue to actually process jobs (and for it to retry ones that
-- time-out).
create :: Job s t => Text -> s -> IO (Hworker s t)
create name state = createWith (defaultHworkerConfig name state)

-- | Create a new worker with a specified 'HworkerConfig'.
--
-- Note that you must create at least one 'worker' and 'monitor' for
-- the queue to actually process jobs (and for it to retry ones that
-- time-out).
createWith :: Job s t => HworkerConfig s -> IO (Hworker s t)
createWith HworkerConfig{..} =
   do conn <- case hwconfigRedisConnectInfo of
                RedisConnectInfo c -> R.connect c
                RedisConnection c -> return c
      return $ Hworker (T.encodeUtf8 hwconfigName)
                       hwconfigState
                       conn
                       hwconfigExceptionBehavior
                       hwconfigLogger
                       hwconfigTimeout
                       hwconfigFailedQueueSize
                       hwconfigDebug

-- | Destroy a worker. This will delete all the queues, clearing out
-- all existing 'jobs', the 'broken' and 'failed' queues. There is no need
-- to do this in normal applications (and most likely, you won't want to).
destroy :: Job s t => Hworker s t -> IO ()
destroy hw = void $ R.runRedis (hworkerConnection hw) $ do
               keys <- withList' hw (R.keys $ "hworker-batch-" <> hworkerName hw <> "*") (void . R.del)
               R.del [ jobQueue hw
                     , progressQueue hw
                     , brokenQueue hw
                     , failedQueue hw
                     ]

jobQueue :: Hworker s t -> ByteString
jobQueue hw = "hworker-jobs-" <> hworkerName hw

progressQueue :: Hworker s t -> ByteString
progressQueue hw = "hworker-progress-" <> hworkerName hw

brokenQueue :: Hworker s t -> ByteString
brokenQueue hw = "hworker-broken-" <> hworkerName hw

failedQueue :: Hworker s t -> ByteString
failedQueue hw = "hworker-failed-" <> hworkerName hw

batchCounter :: Hworker s t -> BatchID -> ByteString
batchCounter hw (BatchID batch) =
  "hworker-batch-" <> hworkerName hw <> ":" <> UUID.toASCIIBytes batch

-- | Adds a job to the queue. Returns whether the operation succeeded.
queue :: Job s t => Hworker s t -> t -> IO Bool
queue hw j =
  do job_id <- UUID.toText <$> UUID.nextRandom
     isRight <$> R.runRedis (hworkerConnection hw)
                (R.lpush (jobQueue hw) [LB.toStrict $ A.encode (JobRef job_id Nothing, j)])

queueBatched :: Job s t => Hworker s t -> t -> BatchID -> Bool -> IO Bool
queueBatched hw j batch finish = do
  job_id <- UUID.toText <$> UUID.nextRandom
  R.runRedis (hworkerConnection hw) $ do
    result <- R.lpush (jobQueue hw) [LB.toStrict $ A.encode (JobRef job_id (Just batch), j)]
    void $ R.hincrby (batchCounter hw batch) "total" 1
    when finish . void $ R.hset (batchCounter hw batch) "status" (encodeBatchStatus BatchProcessing)
    return $ isRight result

-- | Creates a new worker thread. This is blocking, so you will want to
-- 'forkIO' this into a thread. You can have any number of these (and
-- on any number of servers); the more there are, the faster jobs will
-- be processed.
worker :: Job s t => Hworker s t -> IO ()
worker hw =
  do now <- getCurrentTime
     r <- R.runRedis (hworkerConnection hw) $
            R.eval "local job = redis.call('rpop',KEYS[1])\n\
                   \if job ~= nil then\n\
                   \  redis.call('hset', KEYS[2], tostring(job), ARGV[1])\n\
                   \  return job\n\
                   \else\n\
                   \  return nil\n\
                   \end"
                   [jobQueue hw, progressQueue hw]
                   [LB.toStrict $ A.encode now]
     case r of
       Left err -> hwlog hw err >> delayAndRun
       Right Nothing -> delayAndRun
       Right (Just t) ->
         do when (hworkerDebug hw) $ hwlog hw ("WORKER RUNNING", t)
            case decodeValue (LB.fromStrict t) of
              Nothing -> do hwlog hw ("BROKEN JOB", t)
                            now <- getCurrentTime
                            withNil hw (R.eval "local del = redis.call('hdel', KEYS[1], ARGV[1])\n\
                                                \if del == 1 then\n\
                                                \  redis.call('hset', KEYS[2], ARGV[1], ARGV[2])\n\
                                                \end\n\
                                                \return nil"
                                                [progressQueue hw, brokenQueue hw]
                                                [t, LB.toStrict $ A.encode now])
                            delayAndRun
              Just (JobRef _ maybeBatch, j) -> do
                result <- runJob (job (hworkerState hw) j)
                case result of
                  Success ->
                    do when (hworkerDebug hw) $ hwlog hw ("JOB COMPLETE", t)
                       delete_res <- R.runRedis (hworkerConnection hw)
                                                (R.hdel (progressQueue hw) [t])
                       case maybeBatch of
                         Nothing -> return ()
                         Just batch -> incBatchSuccesses hw batch
                       case delete_res of
                         Left err -> hwlog hw err >> delayAndRun
                         Right 1 -> justRun
                         Right n -> do hwlog hw ("Job done: did not delete 1, deleted " <> show n)
                                       delayAndRun
                  Retry msg ->
                    do hwlog hw ("Retry: " <> msg)
                       withNil hw
                                (R.eval "local del = redis.call('hdel', KEYS[1], ARGV[1])\n\
                                        \if del == 1 then\n\
                                        \  redis.call('lpush', KEYS[2], ARGV[1])\n\
                                        \end\n\
                                        \return nil"
                                        [progressQueue hw, jobQueue hw]
                                        [t])
                       case maybeBatch of
                         Nothing -> return ()
                         Just batch -> incBatchRetries hw batch
                       delayAndRun
                  Failure msg ->
                    do hwlog hw ("Failure: " <> msg)
                       withNil hw
                                (R.eval "local del = redis.call('hdel', KEYS[1], ARGV[1])\n\
                                        \if del == 1 then\n\
                                        \  redis.call('lpush', KEYS[2], ARGV[1])\n\
                                        \  redis.call('ltrim', KEYS[2], 0, ARGV[2])\n\
                                        \end\n\
                                        \return nil"
                                        [progressQueue hw, failedQueue hw]
                                        [t, B8.pack (show (hworkerFailedQueueSize hw - 1))])
                       case maybeBatch of
                         Nothing -> return ()
                         Just batch -> incBatchFailures hw batch
                       void $ R.runRedis (hworkerConnection hw)
                                         (R.hdel (progressQueue hw) [t])
                       delayAndRun
  where delayAndRun = threadDelay 10000 >> worker hw
        justRun = worker hw
        runJob v =
          do res <-
               catchJust
                  ( \(e :: SomeException) ->
                      if isJust (asyncExceptionFromException e :: Maybe AsyncException) then
                        Nothing
                      else
                        Just e
                  )
                  ( Right <$> v )
                  ( \e -> return (Left e) )
             case res of
               Left e ->
                 let b = case hworkerExceptionBehavior hw of
                           RetryOnException -> Retry
                           FailOnException -> Failure in
                 return (b ("Exception raised: " <> (T.pack . show) e))
               Right r -> return r


-- | Start a monitor. Like 'worker', this is blocking, so should be
-- started in a thread. This is responsible for retrying jobs that
-- time out (which can happen if the processing thread is killed, for
-- example). You need to have at least one of these running to have
-- the retry happen, but it is safe to have any number running.
monitor :: Job s t => Hworker s t -> IO ()
monitor hw =
  forever $
  do now <- getCurrentTime
     withList hw (R.hkeys (progressQueue hw))
       (\jobs ->
          void $ forM jobs $ \job ->
            withMaybe hw (R.hget (progressQueue hw) job)
             (\start ->
                when (diffUTCTime now (fromJust $ decodeValue (LB.fromStrict start)) > hworkerJobTimeout hw) $
                  do n <-
                       withInt hw
                         (R.eval "local del = redis.call('hdel', KEYS[2], ARGV[1])\n\
                                 \if del == 1 then\
                                 \  redis.call('rpush', KEYS[1], ARGV[1])\n\                                   \end\n\
                                 \return del"
                                 [jobQueue hw, progressQueue hw]
                                 [job])
                     when (hworkerDebug hw) $ hwlog hw ("MONITOR RV", n)
                     when (hworkerDebug hw && n == 1) $ hwlog hw ("MONITOR REQUEUED", job)))
     -- NOTE(dbp 2015-07-25): We check every 1/10th of timeout.
     threadDelay (floor $ 100000 * hworkerJobTimeout hw)

-- | Returns the jobs that could not be deserialized, most likely
-- because you changed the 'ToJSON'/'FromJSON' instances for you job
-- in a way that resulted in old jobs not being able to be converted
-- back from json. Another reason for jobs to end up here (and much
-- worse) is if you point two instances of 'Hworker', with different
-- job types, at the same queue (ie, you re-use the name). Then
-- anytime a worker from one queue gets a job from the other it would
-- think it is broken.
broken :: Hworker s t -> IO [(ByteString, UTCTime)]
broken hw = do r <- R.runRedis (hworkerConnection hw) (R.hgetall (brokenQueue hw))
               case r of
                 Left err -> hwlog hw err >> return []
                 Right xs -> return (map (second parseTime) xs)
  where parseTime = fromJust . decodeValue . LB.fromStrict

jobsFromQueue :: Job s t => Hworker s t -> ByteString -> IO [t]
jobsFromQueue hw queue =
  do r <- R.runRedis (hworkerConnection hw) (R.lrange queue 0 (-1))
     case r of
       Left err -> hwlog hw err >> return []
       Right [] -> return []
       Right xs -> return $ mapMaybe (fmap (\(JobRef _ _, x) -> x) . decodeValue . LB.fromStrict) xs

-- | Returns all pending jobs.
jobs :: Job s t => Hworker s t -> IO [t]
jobs hw = jobsFromQueue hw (jobQueue hw)

-- | Returns all failed jobs. This is capped at the most recent
-- 'hworkerconfigFailedQueueSize' jobs that returned 'Failure' (or
-- threw an exception when 'hworkerconfigExceptionBehavior' is
-- 'FailOnException').
failed :: Job s t => Hworker s t -> IO [t]
failed hw = jobsFromQueue hw (failedQueue hw)

-- | Logs the contents of the jobqueue and the inprogress queue at
-- `microseconds` intervals.
debugger :: Job s t => Int -> Hworker s t -> IO ()
debugger microseconds hw =
  forever $
  do withList hw (R.hkeys (progressQueue hw))
               (\running ->
                  withList hw (R.lrange (jobQueue hw) 0 (-1))
                        (\queued -> hwlog hw ("DEBUG", queued, running)))
     threadDelay microseconds

initBatch :: Hworker s t -> IO BatchID
initBatch hw = do
  batch <- BatchID <$> UUID.nextRandom
  R.runRedis (hworkerConnection hw) $
    R.hmset (batchCounter hw batch)
      [ ("total", "0")
      , ("completed", "0")
      , ("successes", "0")
      , ("failures", "0")
      , ("retries", "0")
      , ("status", "queueing")
      ]
  return batch

batchJob :: Hworker s t -> BatchID -> IO (Maybe BatchJob)
batchJob hw batch = do
  r <- R.runRedis (hworkerConnection hw) (R.hgetall (batchCounter hw batch))
  case r of
    Left err -> hwlog hw err >> return Nothing
    Right hm ->
      return $
        BatchJob
          <$> pure batch
          <*> (lookup "total" hm >>= readMaybe)
          <*> (lookup "completed" hm >>= readMaybe)
          <*> (lookup "successes" hm >>= readMaybe)
          <*> (lookup "failures" hm >>= readMaybe)
          <*> (lookup "retries" hm >>= readMaybe)
          <*> (lookup "status" hm >>= decodeBatchStatus)

incBatchSuccesses :: Hworker s t -> BatchID -> IO ()
incBatchSuccesses hw batch =
  void $ R.runRedis (hworkerConnection hw) $ do
    void $ withInt' hw $ R.hincrby (batchCounter hw batch) "successes" 1
    completeBatch hw batch

incBatchFailures :: Hworker s t -> BatchID -> IO ()
incBatchFailures hw batch =
  void $ R.runRedis (hworkerConnection hw) $ do
    void $ withInt' hw $ R.hincrby (batchCounter hw batch) "failures" 1
    completeBatch hw batch

incBatchRetries :: Hworker s t -> BatchID -> IO ()
incBatchRetries hw batch =
  void $ R.runRedis (hworkerConnection hw) $
    R.hincrby (batchCounter hw batch) "retries" 1

completeBatch :: Hworker s t -> BatchID -> R.Redis ()
completeBatch hw batch = do
  completed <- withInt' hw $ R.hincrby (batchCounter hw batch) "completed" 1
  total <- withInt' hw $ R.hincrby (batchCounter hw batch) "total" 0
  withMaybe' hw (R.hget (batchCounter hw batch) "status") $
    \status ->
      case status of
        "processing" | completed >= total ->
          void $ R.hset (batchCounter hw batch) "status" (encodeBatchStatus BatchFinished)

        _ ->
          return ()


-- Redis helpers follow
withList hw a f =
  do r <- R.runRedis (hworkerConnection hw) a
     case r of
       Left err -> hwlog hw err
       Right [] -> return ()
       Right xs -> f xs

withMaybe hw a f =
  do r <- R.runRedis (hworkerConnection hw) a
     case r of
       Left err -> hwlog hw err
       Right Nothing -> return ()
       Right (Just v) -> f v

withNil hw a =
  do r <- R.runRedis (hworkerConnection hw) a
     case r of
       Left err -> hwlog hw err
       Right (Just ("" :: ByteString)) -> return ()
       Right _ -> return ()

withInt :: Hworker s t -> R.Redis (Either R.Reply Integer) -> IO Integer
withInt hw a =
  do r <- R.runRedis (hworkerConnection hw) a
     case r of
       Left err -> hwlog hw err >> return (-1)
       Right n -> return n

withIgnore :: Hworker s t -> R.Redis (Either R.Reply a) -> IO ()
withIgnore hw a =
  do r <- R.runRedis (hworkerConnection hw) a
     case r of
       Left err -> hwlog hw err
       Right _ -> return ()


withList' hw a f =
  do r <- a
     case r of
       Left err -> liftIO $ hwlog hw err
       Right [] -> return ()
       Right xs -> f xs

withInt' :: Hworker s t -> R.Redis (Either R.Reply Integer) -> R.Redis Integer
withInt' hw a =
  do r <- a
     case r of
       Left err -> liftIO (hwlog hw err) >> return (-1)
       Right n -> return n


withMaybe' :: Hworker s t -> R.Redis (Either R.Reply (Maybe a)) -> (a -> R.Redis ()) -> R.Redis ()
withMaybe' hw a f =
  do r <- a
     case r of
       Left err -> liftIO $ hwlog hw err
       Right Nothing -> return ()
       Right (Just v) -> f v

readMaybe :: Read a => ByteString -> Maybe a
readMaybe =
  fmap fst . listToMaybe . reads . B8.unpack
