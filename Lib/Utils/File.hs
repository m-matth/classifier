
module Lib.Utils.File
  (
    readFile'
  ) where
import System.IO.Unsafe
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy.Internal as BLI

import System.IO

hGetContentsN k h = lazyRead -- TODO close on exceptions
  where
    lazyRead = unsafeInterleaveIO loop

    loop = do
        c <- BS.hGetSome h k -- only blocks if there is no data available
        if BS.null c
          then hClose h >> return BLI.Empty
          else do cs <- lazyRead
                  return (BLI.Chunk c cs)

{-| 'readFile'' allow reading file with specific chunk size
-}
readFile' f buffSz = openBinaryFile f ReadMode >>= hGetContents' buffSz

hGetContents' buffSz = hGetContentsN buffSz
