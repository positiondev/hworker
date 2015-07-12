{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
import           Control.Concurrent       (forkIO, killThread, threadDelay)
import           Control.Concurrent.MVar  (MVar, modifyMVar_, newMVar, putMVar,
                                           takeMVar)
import           Control.Monad            (replicateM_)
import           Data.Aeson               (FromJSON, ToJSON)
import           GHC.Generics             (Generic)
import           System.Hworker
import           System.IO

import           Test.Hspec
import           Test.Hspec.Contrib.HUnit
import           Test.HUnit

data AJob = AJob deriving (Generic, Show)
data AState = AState { unAState :: MVar Int }
instance ToJSON AJob
instance FromJSON AJob
instance Job AState AJob where
  job (AState mvar) AJob = do modifyMVar_ mvar (return . (+1))
                              return Success

data ExJob = ExJob deriving (Generic, Show)
data ExState = ExState { unExState :: MVar Int }
instance ToJSON ExJob
instance FromJSON ExJob
instance Job ExState ExJob where
  job (ExState mvar) ExJob =
    do v <- takeMVar mvar
       putMVar mvar (v + 1)
       if v > 0
          then return Success
          else error "ExJob: failing badly!"

data TimedJob = TimedJob Int deriving (Generic, Show)
data TimedState = TimedState { unTimedState :: MVar Int }
instance ToJSON TimedJob
instance FromJSON TimedJob
instance Job TimedState TimedJob where
  job (TimedState mvar) (TimedJob delay) =
    do threadDelay delay
       modifyMVar_ mvar (return . (+1))
       return Success

nullLogger :: Show a => a -> IO ()
nullLogger = const (return ())

print' :: Show a => a -> IO ()
print' a = do print a
              hFlush stdout

main :: IO ()
main = hspec $
  do describe "Simple job" $
       do it "should run and increment counter" $
            do mvar <- newMVar 0
               hworker <- createWith "aworker-1" (AState mvar) FailOnException nullLogger 4 False
               wthread <- forkIO (worker hworker)
               queue hworker AJob
               threadDelay 30000
               killThread wthread
               destroy hworker
               v <- takeMVar mvar
               assertEqual "State should be 1 after job runs" 1 v
          it "queueing 2 jobs should increment twice" $
            do mvar <- newMVar 0
               hworker <- createWith "aworker-2" (AState mvar) FailOnException nullLogger 4 False
               wthread <- forkIO (worker hworker)
               queue hworker AJob
               queue hworker AJob
               threadDelay 40000
               killThread wthread
               destroy hworker
               v <- takeMVar mvar
               assertEqual "State should be 2 after 2 jobs run" 2 v
          it "queueing 1000 jobs should increment 1000" $
            do mvar <- newMVar 0
               hworker <- createWith "aworker-3" (AState mvar) FailOnException nullLogger 4 False
               wthread <- forkIO (worker hworker)
               replicateM_ 1000 (queue hworker AJob)
               threadDelay 1000000
               killThread wthread
               destroy hworker
               v <- takeMVar mvar
               assertEqual "State should be 1000 after 1000 job runs" 1000 v

     describe "Exceptions" $
       do it "should be able to have exceptions thrown in jobs and retry the job" $
            do mvar <- newMVar 0
               hworker <- createWith "exworker-1" (ExState mvar) RetryOnException nullLogger 4 False
               wthread <- forkIO (worker hworker)
               queue hworker ExJob
               threadDelay 40000
               killThread wthread
               destroy hworker
               v <- takeMVar mvar
               assertEqual "State should be 2, since the first run failed" 2 v
          it "should not retry if mode is FailOnException" $
             do mvar <- newMVar 0
                hworker <- createWith "exworker-2" (ExState mvar) FailOnException nullLogger 4 False
                wthread <- forkIO (worker hworker)
                queue hworker ExJob
                threadDelay 30000
                killThread wthread
                destroy hworker
                v <- takeMVar mvar
                assertEqual "State should be 1, since failing run wasn't retried" 1 v

     describe "Monitor" $
       do it "should add job back after timeout" $
          -- NOTE(dbp 2015-07-12): The timing on this test is somewhat
          -- tricky.  We want to get the job started with one worker,
          -- then kill the worker, then start a new worker, and have
          -- the monitor put the job back in the queue and have the
          -- second worker finish it. It's important that the job
          -- takes less time to complete than the timeout for the
          -- monitor, or else it'll queue it forever.
            do mvar <- newMVar 0
               hworker <- createWith "timedworker-1" (TimedState mvar) FailOnException print' 0.2 False
               wthread1 <- forkIO (worker hworker)
               mthread <- forkIO (monitor hworker)
               queue hworker (TimedJob 100000)
               threadDelay 10000
               killThread wthread1
               wthread2 <- forkIO (worker hworker)
               threadDelay 1000000
               destroy hworker
               v <- takeMVar mvar
               assertEqual "State should be 2, since monitor thinks it failed" 2 v
