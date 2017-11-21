{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}

module Lib.Utils.Bloodhound
  (
    BulkResult(..)
  , MoreLikeThisObj(..)
  , moreLikeThisRequest
  , Same(..)
  , Location(..)
  ) where


import GHC.Generics (Generic)
import Data.Text
import Data.ByteString.Internal

import Data.Aeson (FromJSON (..), ToJSON, defaultOptions,
                   genericParseJSON, genericToJSON, object,
                   encode, (.=))
import Network.HTTP.Client.Internal (Manager, Request(..), Response(..),
                                     parseRequest, httpLbs, RequestBody(..),
                                     applyBasicAuth)

import Database.V5.Bloodhound
import qualified Data.List.NonEmpty as L
import qualified Data.ByteString as S
import qualified Data.ByteString.Lazy   as B


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

{- | 'Same' is an extension  to Bloodhound data type
to describe a more_like_this content resquest
-}
data Same = Same {
  _index :: IndexName
  , _type :: MappingName
  ,_id :: [Char]
  } deriving (Eq, Read, Show, Generic)

{- | 'MoreLikeThisObj' is an extension  to Bloodhound data type
to describe a more_like_this resquest
-}
data MoreLikeThisObj = MoreLikeThisObj {
  fields :: Maybe (L.NonEmpty Text)
  , like :: Maybe (L.NonEmpty Same)
  , min_term_freq :: Integer
  , max_query_terms :: Integer
  } deriving (Show, Generic)

instance ToJSON Same where
  toJSON = genericToJSON defaultOptions

instance ToJSON MoreLikeThisObj where
  toJSON = genericToJSON defaultOptions


{- | 'moreLikeThisRequest' send a more_like_this request
to an elastic search server
-}
moreLikeThisRequest :: Manager
     -> String
     -> String
     -> String
     -> Maybe (L.NonEmpty Text)
     -> Maybe (L.NonEmpty Same)
     -> IO (Response B.ByteString)
moreLikeThisRequest manager host user passwd fields like = do
  -- Create the request
  let requestObject = object [ "query" .= object [ "more_like_this" .= MoreLikeThisObj fields like 1 12 ] ]
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
 , long :: Double
 } deriving (Show, Generic)

instance FromJSON Location where
  parseJSON = genericParseJSON defaultOptions

instance ToJSON Location where
  toJSON = genericToJSON defaultOptions
