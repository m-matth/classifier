{-# LANGUAGE DeriveGeneric #-}

module Lib.Utils.Blocket
  (
    Bsearch(..)
  , BsearchInfo(..)
  , BsearchDocs(..)
  , Docs(..)
  , bSearch
  ) where

import Data.Text (Text, pack, unpack)
import GHC.Generics (Generic)
import Data.List (intercalate)
import Data.Text.Lazy.Encoding
import Data.Aeson (FromJSON (..), ToJSON, defaultOptions,
                    genericParseJSON, genericToJSON,
                    eitherDecode, toJSON)

import qualified Network.Simple.TCP as ST
import qualified Lib.Utils.Socket as US
import qualified Data.ByteString.Internal as BI

data Docs = Docs {
    id :: Text
  , title :: Text
  } deriving (Show, Generic)

instance ToJSON Docs

{-| 'bSearch' send to a request to search engine
-}
bSearch :: [Char] -> [Char] -> [Char] -> IO(Maybe [Docs])
bSearch server port qs = ST.connect server port $ \(sock, remoteAddr) -> do

  print $ "Sending : " ++ show(qs)

  ST.send sock $ BI.packChars $ qs
  rMsg <- US.recv' sock

  case rMsg of
    Nothing -> return Nothing
    Just latinData -> do
      let jsonData = encodeUtf8 $ decodeLatin1 $ latinData
      let json = (eitherDecode $ jsonData) :: (Either String (Bsearch))
      case json of
        Left err -> do
          print $ "Error while decoding bsearch response: " ++ err
          return $ Nothing
        Right ps -> do
          let subjectList = [Docs
                              (list_id (x::BsearchDocs)) $
                              (case (subject (x::BsearchDocs)) of
                                  Nothing -> pack $ ""
                                  Just subject -> subject
                              ) | x <- (docs ps::[BsearchDocs])]
          return $ Just $ subjectList

{-| 'BsearchInfo' is header of a search engine response
-}
data BsearchInfo = BsearchInfo
  {
    lines :: Integer
  , to_id :: Maybe String
  , filtered :: Integer
  } deriving (Show, Generic)

instance FromJSON BsearchInfo where
  parseJSON = genericParseJSON defaultOptions

{-| 'BsearchDocs' is documents content of a search engine response
-}
data BsearchDocs = BsearchDocs
  {
    list_id :: Text
  , subject :: Maybe Text
  , body :: Maybe String
  , list_date :: Maybe Text
  , email :: Maybe Text
  , category :: Maybe Text
  , typ_ :: Maybe Text
  , ad_type :: Maybe Text
  , region :: Maybe Text
  , dpt_code :: Maybe Text
  , name :: Maybe Text
  , image :: Maybe Text
  , phone :: Maybe Text
  , phone_hidden :: Maybe Text
  , no_salesmen :: Maybe Text
  , store_id :: Maybe Text
  , contact_id :: Maybe Text
  , company_ad :: Maybe Text
  , activity_sector :: Maybe Text
  , extra_images :: Maybe Text
  , expiration_time :: Maybe Text
  , orig_date :: Maybe Text
  , last_edited_tm :: Maybe Text
  , date :: Maybe Text
  , city :: Maybe Text
  , address :: Maybe Text
  , price :: Maybe Text
  , zipcode :: Maybe Text
  , siren :: Maybe Text
  , pseudo :: Maybe Text
  , latitude :: Maybe Text
  , longitude :: Maybe Text
  , geo_source :: Maybe Text
  , geo_provider :: Maybe Text
  , imported :: Maybe Text
  , suborder :: Maybe Text
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
