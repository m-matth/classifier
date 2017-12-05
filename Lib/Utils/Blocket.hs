{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}

module Lib.Utils.Blocket
  (
    Bsearch(..)
  , BsearchInfo(..)
  , BsearchDocs(..)
  , BsearchDocsMapping(..)
  , BsearchDocsMapping2(..)
  , Docs(..)
  , bSearch
  , authenticateAdmin
  , newAd
  , reviewAccept
  ) where

import Debug.Trace
import Control.Concurrent.Thread.Delay

import Data.Text.ICU.Convert (toUnicode, open)
import qualified Data.Text.Encoding as E (decodeUtf8, streamDecodeUtf8, decodeLatin1)

import Data.Text (Text, pack, unpack, concat)
import qualified Data.Text.Lazy as TL (concat, unpack, toStrict, Text(..))
import Data.Text.Lazy.Read as TLR (double)
import Data.List.Split (splitOn)
import GHC.Generics (Generic)
import Data.List (intercalate)
import qualified Data.Text.Lazy.Encoding as LE
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
  , loc :: Text
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
      let jsonData = LE.encodeUtf8 $ LE.decodeLatin1 $ latinData
      let json = (eitherDecode $ jsonData) :: (Either String (Bsearch))
      case json of
        Left err -> do
          print $ "Error while decoding bsearch response: " ++ err
          return $ Nothing
        Right ps -> do
          let subjectList = [Docs
                              (list_id (x::BsearchDocs))
                              (case (subject (x::BsearchDocs)) of
                                  Nothing -> pack $ ""
                                  Just subject -> subject
                              )
                              (case (city (x::BsearchDocs)) of
                                  Just "" -> fallbackZipcode x
                                  Nothing -> fallbackZipcode x
                                  Just city -> city
                              )
                            | x <- (docs ps::[BsearchDocs])]
          return $ Just $ subjectList
        where
          fallbackZipcode x =
            case (zipcode (x::BsearchDocs)) of
              Nothing -> pack $ ".."
              Just zipcode -> zipcode

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
  , company_ad :: Maybe Text
  , activity_sector :: Maybe Text
  , extra_images :: Maybe Text
  , expiration_time :: Maybe Text
  , orig_date :: Maybe Text
  , last_edited_tm :: Maybe Text
  , city :: Maybe Text
  , price :: Maybe Text
  , zipcode :: Maybe Text

{- latitude and longitude fields will be converted to
Location during parsing

  , latitude :: Text
  , longitude :: Text

-}
  , location :: UB.Location

  , jobcontract :: Maybe Text
  , jobfield :: Maybe Text
  , jobduty :: Maybe Text
  , jobstudy :: Maybe Text
  , jobtime :: Maybe Text
  , jobexp :: Maybe Text
  } deriving (Show, Generic)

instance ToJSON BsearchDocs where
  toJSON = genericToJSON defaultOptions


data BsearchDocsMapping = BsearchDocsMapping deriving (Eq, Show)

instance ToJSON BsearchDocsMapping where
  toJSON BsearchDocsMapping =
    object
      [ "properties" .=
        object ["location" .= object ["type" .= ("geo_point" :: Text)]]
      ]

data BsearchDocsMapping2 = BsearchDocsMapping2 deriving (Eq, Show)

instance ToJSON BsearchDocsMapping2 where
  toJSON BsearchDocsMapping2 =
    object
      [ "properties" .=
        object ["category" .= object ["similarity" .= ("boolean" :: Text)]]
      ]

defaultLat = 42.0 :: Double
defaultLong = 0.0 :: Double

toDoubleDefault hashMap key _default =
  case (HM.lookup key hashMap) of
    Just (String x) -> case reads $ unpack x :: [(Double, String)] of
                         [(x , y)] -> x
                         [] -> _default
    _ -> _default

jsonConvertLocation :: Value -> Value
jsonConvertLocation (Object o) =
  case (HM.lookup "location" o) of
    Just x -> Object o
    Nothing -> do
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



buildKeyVal key val = unpack $ Data.Text.concat [key , ":" , val, "\n"]

appendField :: Text -> Maybe Text -> String
appendField key val =
  case val of
    Nothing ->  ""
    Just val -> buildKeyVal key val

appendFieldNotZero key val =
  case val of
    Nothing ->  ""
    Just "0" -> ""
    Just val -> buildKeyVal key val


--newAd :: String -> String -> BsearchDocs -> IO(String)
newAd server port doc email = ST.connect server port $ \(sock, _) -> do
  let proAd = (case company_ad doc of
                 Nothing -> "0"
                 Just x -> unpack x)

--  if (proAd == "1") then return "" else do

  conv <- open "ISO-8859-1" Nothing
  let b = BI.packChars $ (appendField "body" (body doc))
  let bodyLatin = E.decodeLatin1 b

  let qs = "cmd:newad\n"
           ++ "company_ad:" ++ proAd ++ "\n"
           ++ "pay_type:none\n"
           ++ appendField "region" (region doc)
           ++ appendFieldNotZero "dpt_code" (dpt_code doc)
           ++ appendField "type" (ad_type doc)
           ++ appendField "category" (category doc)
           ++ "email:" ++ (if proAd == "1" then "matthieu.morel+qa5@schibsted.com" else email) ++ "\n"
           ++ appendField "subject" (subject doc)
           ++ appendField "jobcontract" (jobcontract doc)
           ++ appendField "jobfield" (jobfield doc)
           ++ appendField "jobduty" (jobduty doc)
           ++ appendField "jobstudy" (jobstudy doc)
           ++ appendField "jobtime" (jobtime doc)
           ++ appendField "jobexp" (jobexp doc)
           ++  -- unpack
           unpack bodyLatin
           ++ (if proAd == "1" then appendField "siren" (siren doc) else "")
           ++ (if proAd == "1" then "store:13521\n" else "")
           ++ (if proAd == "1" then "auth_resource:13521\n" else "")
--           ++ (if proAd == "1" then appendField "store" (store_id doc) else "")
           ++ (if proAd == "1" then "auth_type:store\n" else "")
           ++ (if proAd == "1" then "token:0b49eeec-1dd5-4405-9ce1-90a18e2c7e66\n" else "")
--           ++ appendField "pseudo" (pseudo doc)
--           ++ appendField "image.0.name" (image doc)
--           ++ "image.0.digest:091c74518eb959d4b2bfd876e3d3c088ce419eb0\n"
           -- image.0.format.normal.name:1177618189.jpg
           -- image.0.format.normal.namespace:photos
           ++ appendField "price" (price doc)
           ++ appendField "city" (city doc)
           ++ appendField "zipcode" (zipcode doc)
           ++ "do_not_send_mail:1\n"
           ++ "remote_addr:127.0.0.1\n"
           ++ "passwd:0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33\n"
           ++ "geo_source:user\n" ++ "geo_provider:here\n"
           ++ "latitude:" ++ show (UB.lat $ location doc) ++ "\n"
           ++ "longitude:" ++ show (UB.lon $ location doc) ++ "\n"
           ++ "phone:0406020607\n" 
           ++ appendField "name" (name doc)
           ++ "\ncommit:1\nend\n"

--  print $ "Sending : " ++ show(qs)

  ST.send sock $ BI.packChars $ qs
  rMsg <- US.recv' sock

  delay 2000
--  print $ rMsg

  case rMsg of
    Nothing -> return ""
    Just ret -> return "" -- do
{-
      let status = last $ splitOn ":" $ head $ dropWhile (\x -> if ((head $ splitOn ":" x) == "status") then False else True) (splitOn "\n" $ BL8.unpack ret)
      let ad_id = last $ splitOn ":" $ head $ dropWhile (\x -> if ((head $ splitOn ":" x) == "ad_id") then False else True) (splitOn "\n" $ BL8.unpack ret)
      let !action_id = last $ splitOn ":" $ head $ dropWhile (\x -> if ((head $ splitOn ":" x) == "action_id") then False else True) (splitOn "\n" $ BL8.unpack ret)

      {-
      print (if status == "TRANS_ERROR" then "error :" ++ show ret ++ "\nwith query : " ++ qs ++ "\n"
             else "ok : " ++ ad_id ++ "," ++ action_id)
-}
      return $ ad_id -- last $ splitOn ":" $ head $ dropWhile (\x -> if ((head $ splitOn ":" x) == "ad_id") then False else True) (splitOn "\n" $ BL8.unpack ret)
-}


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

