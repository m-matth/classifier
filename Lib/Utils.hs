{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}

module Lib.Utils
  (
    BsearchInfo(..)
  , BsearchDocs(..)
  , Bsearch(..)
  , Job(..)
  , BulkResult(..)
  , Same(..)
  , MoreLikeThisObj(..)
  , moreLikeThisRequest
  ) where

import GHC.Generics           (Generic)
import Data.Text (Text)

import qualified Data.ByteString as S
import qualified Data.ByteString.Lazy   as B
import qualified Data.List.NonEmpty     as L
import Data.Aeson             (FromJSON (..), ToJSON, defaultOptions,
                                genericParseJSON, genericToJSON,
                                object, (.=), eitherDecode, encode,
                                Value(..))
import Database.V5.Bloodhound
import Network.HTTP.Client (defaultManagerSettings)
import Network.HTTP.Client.Internal
import Network.HTTP.Types.Status


{-| 'BsearchInfo' is header of a search engine response
-}
data BsearchInfo = BsearchInfo
  {
    lines :: Integer
  , to_id :: String
  , filtered :: Integer
  } deriving (Show, Generic)

instance FromJSON BsearchInfo where
  parseJSON = genericParseJSON defaultOptions

{-| 'BsearchDocs' is documents content of a search engine response
-}
data BsearchDocs = BsearchDocs
  {
    list_id :: Text
  , subject :: Maybe String
  , body :: Maybe String
  } deriving (Show, Generic)

instance ToJSON BsearchDocs where
  toJSON = genericToJSON defaultOptions
instance FromJSON BsearchDocs where
  parseJSON = genericParseJSON defaultOptions

{-| 'Bsearch' is search engine response (header + contents)
-}
data Bsearch = Bsearch
 {
   info :: Maybe BsearchInfo
 , docs :: [BsearchDocs]
 } deriving (Show, Generic)

instance FromJSON Bsearch where
  parseJSON = genericParseJSON defaultOptions


{-| 'Job' is channel protocol to dispatch documents to threads
-}
data Job = Job {
    ads :: [BsearchDocs]
  , eof :: Bool
  } deriving(Show)




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
  , _type :: [Char]
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
     -> S.ByteString
     -> S.ByteString
     -> Maybe (L.NonEmpty Text)
     -> Maybe (L.NonEmpty Same)
     -> IO (Response B.ByteString)
moreLikeThisRequest manager user passwd fields like = do
  -- Create the request
  let requestObject = object [ "query" .= object [ "more_like_this" .= MoreLikeThisObj fields like 2 12 ] ]

  initialRequest <- applyBasicAuth user passwd <$> parseRequest "http://172.17.0.2:9200/_search"
  let request = initialRequest { method = "POST", requestBody = RequestBodyLBS $ encode requestObject }

  response <- httpLbs request manager
{-  putStrLn $ "The status code was: " ++ (show $ statusCode $ responseStatus response)
  print $ responseBody response -}
  return response
