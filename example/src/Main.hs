{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}


--------------------------------------------------------------------------------
import           Control.Concurrent       ( forkIO, threadDelay )
import           Control.Concurrent.MVar  ( MVar, newMVar, putMVar, takeMVar )
import           Control.Monad            ( forever )
import           Data.Aeson               ( FromJSON, ToJSON )
import           GHC.Generics             ( Generic )
--------------------------------------------------------------------------------
import           System.Hworker
--------------------------------------------------------------------------------


data PrintJob
  = PrintA
  | PrintB
  deriving (Generic, Show)


newtype State =
  State (MVar Int)


instance ToJSON PrintJob
instance FromJSON PrintJob


instance Job State PrintJob where
  job hw PrintA =
    let
      State mvar = hworkerState hw
    in do
    v <- takeMVar mvar
    if v == 0
      then do
        putMVar mvar 0
        putStrLn "A" >> return Success
      else do
        putMVar mvar (v - 1)
        error $ "Dying: " ++ show v

  job _ PrintB =
    putStrLn "B" >> return Success


main :: IO ()
main = do
  mvar <- newMVar 3
  hworker <- create "printer" (State mvar)
  _ <- forkIO (worker hworker)
  _ <- forkIO (monitor hworker)
  _ <- forkIO (forever $ queue hworker PrintA >> threadDelay 1000000)
  _ <- forkIO (forever $ queue hworker PrintB >> threadDelay 500000)
  forever (threadDelay 1000000)
