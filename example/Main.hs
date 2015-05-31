{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
import           Control.Concurrent (forkIO, threadDelay)
import           Control.Monad      (forever)
import           Data.Binary        (Binary)
import           GHC.Generics       (Generic)
import           System.Hworker

data PrintJob = PrintA | PrintB deriving (Generic, Show)
data State = State
instance Binary PrintJob

instance Job State PrintJob where
  job _ PrintA = putStrLn "A" >> return Success
  job _ PrintB = putStrLn "B" >> return Success

main = do hworker <- create "printer" State
          forkIO (worker hworker)
          forkIO (monitor hworker)
          forkIO (forever $ queue hworker PrintA >> threadDelay 1000000)
          forkIO (forever $ queue hworker PrintB >> threadDelay 500000)
          forever (threadDelay 1000000)
