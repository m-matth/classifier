{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}

module Lib.Utils.Bloodhound
  (
    BulkResult(..)
  , MoreLikeThisObj(..)
  , moreLikeThisRequest
  , Same(..)
  , Location(..)
  , PerFieldAnalyzer(..)
  ) where


import GHC.Generics (Generic)
import Data.Text
import Data.ByteString.Internal

import Data.Aeson (FromJSON (..), ToJSON, defaultOptions,
                   genericParseJSON, genericToJSON, object,
                   encode, (.=), Value(..), (.:?))
import Network.HTTP.Client.Internal (Manager, Request(..), Response(..),
                                     parseRequest, httpLbs, RequestBody(..),
                                     applyBasicAuth)

import Database.V5.Bloodhound
import qualified Data.List.NonEmpty as L
import qualified Data.ByteString as S
import qualified Data.ByteString.Lazy   as B

import qualified Data.HashMap.Strict as HM

{-| 'BulkItem' is an extension to Bloodhound data type
to parse bulk response items
-}
data BulkItem = BulkItem {
  _index :: String
  , _type :: String
  , _id :: String
  , _version :: Int
  , result :: String
  , _shards :: ShardResult
  , created :: Bool
  , status :: Int
  } deriving (Eq, Read, Show, Generic)

instance FromJSON BulkItem where
  parseJSON = genericParseJSON defaultOptions

{- | 'BulkIndexItem' is an extension  to Bloodhound data type
to parse bulk index response
-}
data BulkIndexItem = BulkIndexItem {
  index :: BulkItem
  } deriving (Eq, Read, Show, Generic)

instance FromJSON BulkIndexItem where
  parseJSON = genericParseJSON defaultOptions

{- | 'BulkResult' is an extension  to Bloodhound data type
to parse bulk command response
-}
data BulkResult = BulkResult {
  took  :: Int
  , errors :: Bool
  , items :: [BulkIndexItem]
  } deriving (Eq, Read, Show, Generic)

instance FromJSON BulkResult where
  parseJSON = genericParseJSON defaultOptions


data PerFieldAnalyzer = PerFieldAnalyzer {
  field :: Text
  , analyzer :: Analyzer
  } deriving (Eq, Read, Show, Generic)

{- | 'Same' is an extension  to Bloodhound data type
to describe a more_like_this content resquest
-}
data Same = Same {
  _index :: IndexName
  , _type :: MappingName
  ,_id :: [Char]
  , per_field_analyzer :: PerFieldAnalyzer
  } deriving (Eq, Read, Show, Generic)

{- | 'MoreLikeThisObj' is an extension  to Bloodhound data type
to describe a more_like_this resquest
-}
data MoreLikeThisObj = MoreLikeThisObj {
  fields :: Maybe (L.NonEmpty Text)
  , like :: Maybe (L.NonEmpty Same)
  , min_term_freq :: Integer
  , max_query_terms :: Integer
  , min_word_length :: Integer
  , include :: Bool
  , minimum_should_match :: String
  } deriving (Show, Generic)


defField :: Text
defField = "body"

defAnalyzer :: Text
defAnalyzer = "french"

instance ToJSON Same where
  toJSON (Same _index _type _id pfa) =
    object ["_index" .= _index,
            "_type" .= _type,
            "_id" .= _id -- ,
            "per_field_analyzer" .= object [ defField .= defAnalyzer ]
           ]

instance ToJSON MoreLikeThisObj where
  toJSON = genericToJSON defaultOptions

defaultSize :: Integer
defaultSize = 100

{- | 'moreLikeThisRequest' send a more_like_this request
to an elastic search server
-}
moreLikeThisRequest :: Manager
     -> String
     -> String
     -> String
     -> Maybe (L.NonEmpty Text)
     -> Maybe (L.NonEmpty Same)
     -> Bool
     -> IO (Response B.ByteString)
moreLikeThisRequest manager host user passwd fields like include = do
  -- Create the request
  let requestObject = object [
        "query" .= object [
            "more_like_this" .= MoreLikeThisObj fields like 1 10 3 include "30%"
            ],
          "size" .= defaultSize
        ]
  let u = packChars user
  let p = packChars passwd
  initialRequest <- applyBasicAuth u p <$> (parseRequest $ host ++ "/_search")
  let request = initialRequest { method = "POST", requestBody = RequestBodyLBS $ encode requestObject }

  response <- httpLbs request manager
{-  putStrLn $ "The status code was: " ++ (show $ statusCode $ responseStatus response)
  print $ responseBody response -}
  return response


data Location = Location
 {
   lat :: Double
 , lon :: Double
 } deriving (Show, Generic)

instance FromJSON Location where
  parseJSON = genericParseJSON defaultOptions

instance ToJSON Location where
  toJSON = genericToJSON defaultOptions
