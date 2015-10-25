module Data.Aeson.Helpers where

import           Data.Aeson
import           Data.Aeson.Parser    (value)
import           Data.Attoparsec.Lazy (Parser)
import qualified Data.Attoparsec.Lazy as L
import qualified Data.ByteString.Lazy as L

-- NOTE(dbp 2015-06-14): Taken from Data.Aeson.Parser.Internal
decodeWith :: Parser Value -> (Value -> Result a) -> L.ByteString -> Maybe a
decodeWith p to s =
    case L.parse p s of
      L.Done _ v -> case to v of
                      Success a -> Just a
                      _         -> Nothing
      _          -> Nothing
{-# INLINE decodeWith #-}

decodeValue :: FromJSON t => L.ByteString -> Maybe t
decodeValue = decodeWith value fromJSON
