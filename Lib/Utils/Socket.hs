
module Lib.Utils.Socket
  (
    recv'
  ) where

import System.IO
import Network.Socket
import qualified Data.ByteString.Lazy as BSL

{-| 'recv'' is a workaround for Network.Simple.TCP
allow to received many data from server which close connection
before data fully treated by client.
-}
recv' sock = do
  h <- socketToHandle sock ReadWriteMode
  hSetBuffering h NoBuffering

  bs <- BSL.hGetContents h
  if BSL.null bs
    then return Nothing
    else return (Just bs)

