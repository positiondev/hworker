{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
import           Control.Concurrent       (forkIO, killThread, threadDelay)
import           Control.Concurrent.MVar  (MVar, modifyMVar_, newMVar, takeMVar)
import           Data.Aeson               (FromJSON, ToJSON)
import           GHC.Generics             (Generic)
import           System.Hworker

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

main :: IO ()
main = hspec $ describe "AJob" $
       do it "should run and increment counter" $
            do mvar <- newMVar 0
               hworker <- create "aworker-1" (AState mvar)
               wthread <- forkIO (worker hworker)
               queue hworker AJob
               threadDelay 30000
               killThread wthread
               v <- takeMVar mvar
               assertEqual "State should be 1 after job runs" 1 v
          it "queueing 2 jobs should increment twice" $
            do mvar <- newMVar 0
               hworker <- create "aworker-2" (AState mvar)
               wthread <- forkIO (worker hworker)
               queue hworker AJob
               queue hworker AJob
               threadDelay 40000
               killThread wthread
               v <- takeMVar mvar
               assertEqual "State should be 1 after job runs" 2 v
