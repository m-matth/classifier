{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}

module Lib.Utils
  (
    Job(..)
  ) where

import Lib.Utils.Blocket (BsearchDocs(..))

{-| 'Job' is channel protocol to dispatch documents to threads
-}
data Job = Job {
    ads :: [BsearchDocs]
  , eof :: Bool
  } deriving(Show)

