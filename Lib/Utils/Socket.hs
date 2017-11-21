
module Lib.Utils.Socket
  (
    recv'
  ) where

import System.IO
import Network.Socket
import qualified Data.ByteString.Lazy as BL

{-| 'recv'' is a workaround for Network.Simple.TCP
allow to received many data from server which close connection
before data fully treated by client.
-}
recv' :: Socket -> IO(Maybe BL.ByteString)
recv' sock = do
  h <- socketToHandle sock ReadWriteMode
  hSetBuffering h NoBuffering

  bs <- BL.hGetContents h
  if BL.null bs
    then return Nothing
    else return (Just bs)

