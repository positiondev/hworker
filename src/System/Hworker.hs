{-# LANGUAGE DeriveGeneric             #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FunctionalDependencies    #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE RecordWildCards           #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE StandaloneDeriving        #-}
module System.Hworker
       ( Result(..)
       , Job(..)
       , Hworker
       , ExceptionBehavior(..)
       , RedisConnection(..)
       , HworkerConfig(..)
       , defaultHworkerConfig
       , create
       , createWith
       , destroy
       , queue
       , worker
       , monitor
       , jobs
       , failed
       , broken
       , debugger
       )
       where

import           Control.Arrow           ((***))
import           Control.Concurrent      (forkIO, threadDelay)
import           Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import           Control.Exception       (SomeException, catch)
import           Control.Monad           (forM, forever, void, when)
import           Data.Aeson              (FromJSON, ToJSON)
import qualified Data.Aeson              as A
import           Data.Aeson.Helpers
import           Data.ByteString         (ByteString)
import qualified Data.ByteString.Char8   as B8
import qualified Data.ByteString.Lazy    as LB
import           Data.Maybe              (catMaybes, fromJust)
import           Data.Monoid             ((<>))
import           Data.Text               (Text)
import qualified Data.Text               as T
import qualified Data.Text.Encoding      as T
import           Data.Time.Calendar      (Day (..))
import           Data.Time.Clock         (NominalDiffTime, UTCTime (..),
                                          diffUTCTime, getCurrentTime)
import qualified Data.UUID               as UUID
import qualified Data.UUID.V4            as UUID
import qualified Database.Redis          as R
import           GHC.Generics            (Generic)


data Result = Success
            | Retry Text
            | Failure Text
            deriving (Generic, Show)
instance ToJSON Result
instance FromJSON Result

class (FromJSON t, ToJSON t, Show t) => Job s t | s -> t where
  job :: s -> t -> IO Result

data JobData t = JobData UTCTime t

data ExceptionBehavior = RetryOnException | FailOnException

hwlog :: Show a => Hworker s t -> a -> IO ()
hwlog hw a = hworkerLogger hw (hworkerName hw, a)

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

data RedisConnection = RedisConnectInfo R.ConnectInfo
                     | RedisConnection R.Connection

data HworkerConfig s =
     HworkerConfig {
         hwconfigName              :: Text
       , hwconfigState             :: s
       , hwconfigRedisConnectInfo  :: RedisConnection
       , hwconfigExceptionBehavior :: ExceptionBehavior
       , hwconfigLogger            :: (forall a. Show a => a -> IO ())
       , hwconfigTimeout           :: NominalDiffTime
       , hwconfigFailedQueueSize   :: Int
       , hwconfigDebug             :: Bool
       }

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

create :: Job s t => Text -> s -> IO (Hworker s t)
create name state = createWith (defaultHworkerConfig name state)

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


destroy :: Job s t => Hworker s t -> IO ()
destroy hw = void $ R.runRedis (hworkerConnection hw) $
               R.del [(jobQueue hw), (progressQueue hw), (brokenQueue hw), (failedQueue hw)]

jobQueue :: Hworker s t -> ByteString
jobQueue hw = "hworker-jobs-" <> hworkerName hw

progressQueue :: Hworker s t -> ByteString
progressQueue hw = "hworker-progress-" <> hworkerName hw

brokenQueue :: Hworker s t -> ByteString
brokenQueue hw = "hworker-broken-" <> hworkerName hw

failedQueue :: Hworker s t -> ByteString
failedQueue hw = "hworker-failed-" <> hworkerName hw

queue :: Job s t => Hworker s t -> t -> IO ()
queue hw j =
  do job_id <- UUID.toString <$> UUID.nextRandom
     void $ R.runRedis (hworkerConnection hw) $
            R.lpush (jobQueue hw) [LB.toStrict $ A.encode (job_id, j)]

worker :: Job s t => Hworker s t -> IO ()
worker hw =
  do now <- getCurrentTime
     r <- R.runRedis (hworkerConnection hw) $
            R.eval "local job = redis.call('rpop',KEYS[1])\n\
                   \if job ~= nil then\n\
                   \  redis.call('hset', KEYS[2], job, ARGV[1])\n\
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
              Just (_ :: String, j) -> do
                result <- runJob (job (hworkerState hw) j)
                case result of
                  Success ->
                    do when (hworkerDebug hw) $ hwlog hw ("JOB COMPLETE", t)
                       delete_res <- R.runRedis (hworkerConnection hw)
                                                (R.hdel (progressQueue hw) [t])
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
                       void $ R.runRedis (hworkerConnection hw)
                                         (R.hdel (progressQueue hw) [t])
                       delayAndRun
  where delayAndRun = threadDelay 10000 >> worker hw
        justRun = worker hw
        runJob v =
          do x <- newEmptyMVar
             jt <- forkIO (catch (v >>= putMVar x . Right)
                                 (\(e::SomeException) ->
                                    putMVar x (Left e)))
             res <- takeMVar x
             case res of
               Left e ->
                 let b = case hworkerExceptionBehavior hw of
                           RetryOnException -> Retry
                           FailOnException -> Failure in
                 return (b ("Exception raised: " <> (T.pack . show) e))
               Right r -> return r

monitor :: Job s t => Hworker s t -> IO ()
monitor hw =
  forever $
  do now <- getCurrentTime
     withList hw (R.hkeys (progressQueue hw))
       (\jobs ->
          do void $ forM jobs $ \job ->
              withMaybe hw (R.hget (progressQueue hw) job)
               (\start ->
                  do when (diffUTCTime now (fromJust $ decodeValue (LB.fromStrict start)) > (hworkerJobTimeout hw)) $
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
     threadDelay (floor $ 100000 * (hworkerJobTimeout hw))

broken :: Hworker s t -> IO [(ByteString, UTCTime)]
broken hw = do r <- R.runRedis (hworkerConnection hw) (R.hgetall (brokenQueue hw))
               case r of
                 Left err -> hwlog hw err >> return []
                 Right xs -> return (map (id *** parseTime) xs)
  where parseTime = fromJust . decodeValue . LB.fromStrict

jobsFromQueue :: Job s t => Hworker s t -> ByteString -> IO [t]
jobsFromQueue hw queue =
  do r <- R.runRedis (hworkerConnection hw) (R.lrange queue 0 (-1))
     case r of
       Left err -> hwlog hw err >> return []
       Right [] -> return []
       Right xs -> return $ catMaybes $ map (fmap (\(_::String, x) -> x) . decodeValue . LB.fromStrict) xs

jobs :: Job s t => Hworker s t -> IO [t]
jobs hw = jobsFromQueue hw (jobQueue hw)

failed :: Job s t => Hworker s t -> IO [t]
failed hw = jobsFromQueue hw (failedQueue hw)

debugger :: Job s t => Int -> Hworker s t -> IO ()
debugger microseconds hw =
  forever $
  do withList hw (R.hkeys (progressQueue hw))
               (\running ->
                  withList hw (R.lrange (jobQueue hw) 0 (-1))
                        (\queued -> hwlog hw ("DEBUG", queued, running)))
     threadDelay microseconds

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
