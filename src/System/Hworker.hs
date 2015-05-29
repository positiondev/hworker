{-# LANGUAGE DeriveGeneric             #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE StandaloneDeriving        #-}
module System.Hworker
       ( Result(..)
       , Job(..)
       , queue
       , worker
       , monitor
       )
       where

import           Control.Exception (SomeException, catch)
import           Data.Binary
import           Data.Monoid       ((<>))
import           Data.Text         (Text)
import qualified Data.Text         as T
import           Data.Time.Clock   (UTCTime)
import qualified Database.Redis    as R
import           GHC.Generics      (Generic)

data Result = Success | Retry Text | Failure Text deriving (Generic, Show)
instance Binary Result

class (Binary t, Show t) => Job t where
  job :: t -> IO Result
  ran :: t -> (Result -> IO ())
  ran _ = const (return ())

data Complete t = Complete t Result

instance Show t => Show (Complete t) where
   show (Complete t r) = "Complete " <> show t <> " " <> show r

instance Binary t => Binary (Complete t) where
   put (Complete t r) = put (t,r)
   get = uncurry Complete <$> get


instance Job t => Job (Complete t) where
  job (Complete t r) = catch (ran t r >> return Success)
                             (\(e :: SomeException) ->
                                return (Failure (T.pack $ "`ran` on Job " <>
                                                          show t <>
                                                          " raised exception: " <>
                                                          show e)))

data JobData t = JobData UTCTime t

queue :: Job t => t -> IO ()
queue job = undefined

worker :: Text -> IO ()
worker = undefined

monitor :: Text -> IO ()
monitor = undefined
