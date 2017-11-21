{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

module Lib.Utils.Blocket
  (
    Bsearch(..)
  , BsearchInfo(..)
  , BsearchDocs(..)
  , Docs(..)
  , bSearch
  , authenticateAdmin
  , newAd
  , reviewAccept
  ) where

import Debug.Trace

import Data.Text (Text, pack, unpack, concat)
import qualified Data.Text.Lazy as TL (concat, unpack, toStrict, Text(..))
import Data.Text.Lazy.Read as TLR (double)
import Data.List.Split (splitOn)
import GHC.Generics (Generic)
import Data.List (intercalate)
import Data.Text.Lazy.Encoding
import Data.Aeson.Text
import Data.Aeson {-(FromJSON (..), ToJSON, defaultOptions,
                    genericParseJSON, genericToJSON,
                    eitherDecode, toJSON, withObject,
                    (.:), (.:?), fieldLabelModifier)
-}

import qualified Data.HashMap.Strict as HM
import Data.Char (toLower)

import qualified Network.Simple.TCP as ST
import qualified Lib.Utils.Socket as US
import qualified Lib.Utils.Bloodhound as UB
import qualified Data.ByteString.Internal as BI
import qualified Data.ByteString.Lazy.Char8 as BL8

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
  , body :: Maybe Text
  , list_date :: Maybe Text
  , category :: Maybe Text
  , region :: Maybe Text
  , dpt_code :: Maybe Text
  , ad_type :: Maybe Text
  , name :: Maybe Text
  , siren :: Maybe Text
  , pseudo :: Maybe Text
  , image :: Maybe Text
  , no_salesmen :: Maybe Text
  , store_id :: Maybe Text
  , contact_id :: Maybe Text
  , company_ad :: Text
  , activity_sector :: Maybe Text
  , extra_images :: Maybe Text
  , expiration_time :: Maybe Text
  , orig_date :: Maybe Text
  , last_edited_tm :: Maybe Text
  , city :: Maybe Text
  , price :: Maybe Text
  , zipcode :: Maybe Text

{- Will be converted to Location during parsing 
  , latitude :: Text
  , longitude :: Text
-}
  , location :: UB.Location

  } deriving (Show, Generic)

instance ToJSON BsearchDocs where
  toJSON = genericToJSON defaultOptions


defaultLat = 42.0 :: Double
defaultLong = 0.0 :: Double

toDoubleDefault hashMap key _default =
  case (HM.lookup key hashMap) of
    Just (String x) -> case reads $ unpack x :: [(Double, String)] of
                         [(x , y)] -> x
                         [] -> _default
    _ -> _default

jsonConvertLocation :: Value -> Value
jsonConvertLocation (Object o) = do
  let location = UB.Location
                 (toDoubleDefault o "latitude" defaultLat)
                 (toDoubleDefault o "longitude" defaultLong)

  Object $ HM.insert "location" (toJSON location) (o::HM.HashMap Text Value)

instance FromJSON BsearchDocs where
  parseJSON = genericParseJSON opts . jsonConvertLocation
    where
      opts = defaultOptions


{-| 'Bsearch' is search engine response (header + contents)
-}
data Bsearch = Bsearch
 {
   info :: Maybe BsearchInfo
 , docs :: [BsearchDocs]
 } deriving (Show, Generic)

instance FromJSON Bsearch where
  parseJSON = genericParseJSON defaultOptions


authenticateAdmin :: String -> String -> String -> String -> IO(String)
authenticateAdmin server port user passwd =
  ST.connect server port $ \(sock, _) -> do
  let qs = "cmd:authenticate\nremote_addr:127.0.0.42"
        ++ "\nusername:" ++ user
        ++ "\npasswd:" ++ passwd
        ++ "\ncommit:1\nend\n"

  print $ "Sending : " ++ show(qs)

  ST.send sock $ BI.packChars $ qs
  rMsg <- US.recv' sock

  case rMsg of
    Nothing -> return ""
    Just ret ->
      return $ last $ splitOn ":" $ head $ dropWhile (\x -> if ((head $ splitOn ":" x) == "token") then False else True) (splitOn "\n" $ BL8.unpack ret)


appendField :: Text -> Maybe Text -> String
appendField key field =
  case (field) of
    Nothing ->  ""
    Just field -> unpack $ Data.Text.concat [key , ":" , field, "\n"]


--newAd :: String -> String -> BsearchDocs -> IO(String)
newAd server port doc = ST.connect server port $ \(sock, _) -> do
  let qs = "cmd:newad\n"
           ++ "company_ad:" ++ unpack (company_ad doc) ++ "\n"
           ++ "pay_type:verify\n"
           ++ appendField "region" (region doc)
           ++ appendField "dpt_code" (dpt_code doc)
           ++ appendField "type" (ad_type doc)
           ++ appendField "category" (category doc)
           ++ "email:foo@foo.org\n"
           ++ appendField "subject" (subject doc)
           ++ appendField "body" (body doc)
           ++ (if (company_ad doc) == "1" then appendField "siren" (siren doc) else "")
           ++ (if (company_ad doc) == "1" then "store_id:1\n" else "")
--           ++ appendField "pseudo" (pseudo doc)
--           ++ appendField "image.0.name" (image doc)
--           ++ "image.0.digest:091c74518eb959d4b2bfd876e3d3c088ce419eb0\n"
           -- image.0.format.normal.name:1177618189.jpg
           -- image.0.format.normal.namespace:photos
           ++ appendField "price" (price doc)
           ++ appendField "city" (city doc)
           ++ appendField "zipcode" (zipcode doc)
           ++ "geo_source:user\n" ++ "geo_provider:here\n"
           ++ "latitude:" ++ show (UB.lat $ location doc) ++ "\n"
           ++ "longitude:" ++ show (UB.long $ location doc) ++ "\n"
           ++ "phone:0406020607\n" 
           ++ appendField "name" (name doc)
           ++ "\ncommit:1\nend\n"

--  print $ "Sending : " ++ show(qs)

  ST.send sock $ BI.packChars $ qs
  rMsg <- US.recv' sock

--  print $ rMsg

  case rMsg of
    Nothing -> return ""
    Just ret ->
      return $ last $ splitOn ":" $ head $ dropWhile (\x -> if ((head $ splitOn ":" x) == "ad_id") then False else True) (splitOn "\n" $ BL8.unpack ret)



reviewAccept server port adId token = ST.connect server port $ \(sock, _) -> do
  let qs = "cmd:review\nad_id:" ++ adId ++ "\naction_id:1\naction:accept\nremote_addr:127.0.0.1\nremote_port:33333\nauth_type:admin\ntoken:" ++ token ++ "\ncommit:1\nend\n"

--  print $ "Sending : " ++ show(qs)

  ST.send sock $ BI.packChars $ qs
  rMsg <- US.recv' sock

--  print $ rMsg

  case rMsg of
    Nothing -> return ""
    Just ret ->
      return $ last $ splitOn ":" $ head $ dropWhile (\x -> if ((head $ splitOn ":" x) == "status") then False else True) (splitOn "\n" $ BL8.unpack ret)

