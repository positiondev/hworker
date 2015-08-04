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
       , debugger
       )
       where

import           Control.Concurrent      (forkIO, threadDelay)
import           Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import           Control.Exception       (SomeException, catch)
import           Control.Monad           (forM, forever, void, when)
import           Data.Aeson              (FromJSON, ToJSON)
import qualified Data.Aeson              as A
import           Data.Aeson.Helpers
import           Data.ByteString         (ByteString)
import qualified Data.ByteString.Lazy    as LB
import           Data.Maybe              (fromJust)
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
hwlog hw a = hworkerLogger hw a

data Hworker s t =
     Hworker { hworkerName              :: ByteString
             , hworkerState             :: s
             , hworkerConnection        :: R.Connection
             , hworkerExceptionBehavior :: ExceptionBehavior
             , hworkerLogger            :: forall a. Show a => a -> IO ()
             , hworkerJobTimeout        :: NominalDiffTime
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
       , hwconfigDebug             :: Bool
       }

defaultHworkerConfig :: Text -> s -> HworkerConfig s
defaultHworkerConfig name state =
  HworkerConfig name
                state
                (RedisConnectInfo R.defaultConnectInfo)
                RetryOnException
                print
                120
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
                       hwconfigDebug


destroy :: Job s t => Hworker s t -> IO ()
destroy hw = void $ R.runRedis (hworkerConnection hw) $
               R.del [(jobQueue hw), (progressQueue hw)]

jobQueue :: Hworker s t -> ByteString
jobQueue hw = "hworker-jobs-" <> hworkerName hw

progressQueue :: Hworker s t -> ByteString
progressQueue hw = "hworker-progress-" <> hworkerName hw

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
         do when (hworkerDebug hw) $ hwlog hw ("WORKER RUNNING", r)
            let (_ :: String, j) = fromJust $ decodeValue (LB.fromStrict t)
            result <- runJob (job (hworkerState hw) j)
            case result of
              Success ->
                do when (hworkerDebug hw) $ hwlog hw ("JOB COMPLETE", t)
                   delete_res <- R.runRedis (hworkerConnection hw)
                                            (R.hdel (progressQueue hw) [t])
                   case delete_res of
                     Left err -> hwlog hw err >> delayAndRun
                     Right 1 -> justRun
                     Right n -> do hwlog hw ("Delete: did not delete 1, deleted " <> show n)
                                   delayAndRun
              Retry msg ->
                do hwlog hw ("Retry: " <> msg)
                   debugNil hw
                            (R.eval "local del = redis.call('hdel', KEYS[1], ARGV[1])\n\
                                    \if del == 1 then\
                                    \  redis.call('lpush', KEYS[2], ARGV[1])\n\
                                    \end\n\
                                    \return nil"
                                    [progressQueue hw, jobQueue hw]
                                    [t])
                   delayAndRun
              Failure msg ->
                do hwlog hw ("Fail: " <> msg)
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

debugList hw a f =
  do r <- R.runRedis (hworkerConnection hw) a
     case r of
       Left err -> hwlog hw err
       Right [] -> return ()
       Right xs -> f xs

debugMaybe hw a f =
  do r <- R.runRedis (hworkerConnection hw) a
     case r of
       Left err -> hwlog hw err
       Right Nothing -> return ()
       Right (Just v) -> f v

debugNil hw a =
  do r <- R.runRedis (hworkerConnection hw) a
     case r of
       Left err -> hwlog hw err
       Right (Just ("" :: ByteString)) -> return ()
       Right _ -> return ()

debugInt :: Hworker s t -> R.Redis (Either R.Reply Integer) -> IO Integer
debugInt hw a =
  do r <- R.runRedis (hworkerConnection hw) a
     case r of
       Left err -> hwlog hw err >> return (-1)
       Right n -> return n

debugger :: Job s t => Int -> Hworker s t -> IO ()
debugger microseconds hw =
  forever $
  do debugList hw (R.hkeys (progressQueue hw))
               (\running ->
                  debugList hw (R.lrange (jobQueue hw) 0 (-1))
                        (\queued -> hwlog hw ("DEBUG", queued, running)))
     threadDelay microseconds

monitor :: Job s t => Hworker s t -> IO ()
monitor hw =
  forever $
  do now <- getCurrentTime
     debugList hw (R.hkeys (progressQueue hw))
       (\jobs ->
          do void $ forM jobs $ \job ->
              debugMaybe hw (R.hget (progressQueue hw) job)
               (\start ->
                  do when (diffUTCTime now (fromJust $ decodeValue (LB.fromStrict start)) > (hworkerJobTimeout hw)) $
                       do n <-
                            debugInt hw
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
