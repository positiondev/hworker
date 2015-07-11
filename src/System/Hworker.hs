{-# LANGUAGE DeriveGeneric             #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FunctionalDependencies    #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE StandaloneDeriving        #-}
module System.Hworker
       ( Result(..)
       , Job(..)
       , Hworker
       , create
       , queue
       , worker
       , monitor
       )
       where

import           Control.Concurrent   (forkIO, threadDelay)
import           Control.Exception    (SomeException, catch)
import           Control.Monad        (forM, forever, void, when)
import           Data.Aeson           (FromJSON, ToJSON)
import qualified Data.Aeson           as A
import           Data.Aeson.Helpers
import           Data.ByteString      (ByteString)
import qualified Data.ByteString.Lazy as LB
import           Data.Maybe           (fromJust)
import           Data.Monoid          ((<>))
import           Data.Text            (Text)
import qualified Data.Text            as T
import qualified Data.Text.Encoding   as T
import           Data.Time.Calendar   (Day (..))
import           Data.Time.Clock      (NominalDiffTime, UTCTime (..),
                                       diffUTCTime, getCurrentTime)
import qualified Database.Redis       as R
import           GHC.Generics         (Generic)


data Result = Success
            | Retry Text
            | Failure Text
            deriving (Generic, Show)
instance ToJSON Result
instance FromJSON Result

class (FromJSON t, ToJSON t, Show t) => Job s t | s -> t where
  job :: s -> t -> IO Result

data JobData t = JobData UTCTime t

data Hworker s t = Hworker { hworkerName       :: ByteString
                           , hworkerState      :: s
                           , hworkerConnection :: R.Connection
                           }

create :: Job s t => Text -> s -> IO (Hworker s t)
create name state = do conn <- R.connect R.defaultConnectInfo
                       return $ Hworker (T.encodeUtf8 name) state conn

jobQueue :: Hworker s t -> ByteString
jobQueue hw = "hworker-jobs-" <> hworkerName hw

progressQueue :: Hworker s t -> ByteString
progressQueue hw = "hworker-progress-" <> hworkerName hw

queue :: Job s t => Hworker s t -> t -> IO ()
queue hw j = void $ R.runRedis (hworkerConnection hw) $
              R.lpush (jobQueue hw) [LB.toStrict $ A.encode j]

worker :: Job s t => Hworker s t -> IO ()
worker hw =
  forever $
  do forkIO $
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
            Left err -> print err
            Right Nothing -> return ()
            Right (Just t) ->
              do result <- job (hworkerState hw) (fromJust $ decodeValue (LB.fromStrict t))
                 case result of
                   Success -> do delete_res <- R.runRedis (hworkerConnection hw)
                                                          (R.hdel (progressQueue hw) [t])
                                 case delete_res of
                                   Left err -> print err
                                   Right 1 -> return ()
                                   Right n -> print ("Delete: did not delete 1, deleted " <> show n)
                   Retry msg -> do print ("Retry: " <> msg)
                                   return ()
                   Failure msg -> do print ("Fail: " <> msg)
                                     void $ R.runRedis (hworkerConnection hw)
                                                       (R.hdel (progressQueue hw) [t])
     threadDelay 10000

timeout :: NominalDiffTime
timeout = 4

debugList hw a f =
  do r <- R.runRedis (hworkerConnection hw) a
     case r of
       Left err -> print err
       Right [] -> return ()
       Right xs -> f xs

debugMaybe hw a f =
  do r <- R.runRedis (hworkerConnection hw) a
     case r of
       Left err -> print err
       Right Nothing -> return ()
       Right (Just v) -> f v

debugNil hw a =
  do r <- R.runRedis (hworkerConnection hw) a
     case r of
       Left err -> print err
       Right (Just ("" :: ByteString)) -> return ()
       Right _ -> return ()

monitor :: Job s t => Hworker s t -> IO ()
monitor hw =
  forever $
  do now <- getCurrentTime
     debugList hw (R.hkeys (progressQueue hw))
       (\jobs ->
          void $ forM jobs $ \job ->
            debugMaybe hw (R.hget (progressQueue hw) job)
               (\start ->
                  when (diffUTCTime now (fromJust $ decodeValue (LB.fromStrict start)) > timeout) $
                   do debugNil hw (R.eval "redis.call('rpush', KEYS[1], ARGV[2])\n\
                                          \redis.call('hdel', KEYS[2], ARGV[1])\n\
                                          \return nil"
                                          [jobQueue hw, progressQueue hw]
                                          [start, job])))
