{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}

import GHC.Generics (Generic)
import Control.Monad.IO.Class

import qualified Data.Aeson as A (FromJSON (..), eitherDecode)

import Data.Text              (Text, pack, unpack)
import Data.List.Split as S
import Data.List (intercalate)
import Data.Proxy (Proxy(..))

import qualified Data.Vector as V

import Database.V5.Bloodhound as B (Server(..), ToJSON(..), SearchResult(..),
                                    bulk, hitDocId, unpackId, hits,
                                    BulkOperation(..), MappingName(..),
                                    IndexName(..), DocId(..), runBH,
                                    bhRequestHook, mkBHEnv, basicAuthHook,
                                    EsUsername(..), EsPassword(..),
                                    updateIndexAliases, IndexAliasAction(..),
                                    defaultIndexSettings, createIndex,
                                    IndexAliasName(..), IndexAlias(..),
                                    IndexAliasCreate(..))

import Network.HTTP.Client (defaultManagerSettings,
                             newManager,
                             responseBody, Manager(..))

import Control.Concurrent
import Control.Concurrent.Chan

import qualified Data.List.NonEmpty as L

import Network.Wai.Handler.Warp as W (run)
import Network.Wai.Middleware.Cors
import Network.Wai.Middleware.Servant.Options

import Servant.Server as S
import Servant.API

import Data.Text.Lazy.Encoding as LE

import qualified Lib.Utils.File as UF (readFile')
import qualified Lib.Utils.Blocket as B (bSearch, Docs(..)
                         , BsearchDocs(..), Bsearch(..), BsearchInfo(..))

import Lib.Utils.Bloodhound (moreLikeThisRequest, BulkResult(..),
                            Same(..))

import System.Console.CmdArgs

import Data.JsonStream.Parser (parseLazyByteString, objectWithKey, value, arrayOf)

nbWorkers = 10

jsonHandle docs runBH' idx alias map = do
  lock <- newMVar ()
  let atomicPutStrLn str = withMVar lock (\_ -> putStrLn str)
  atomicPutStrLn $ "Launch " ++ show(nbWorkers) ++ " to handle data"

  c  <- newChan

  threads <- mapM (\x -> do let
                            feedback <- newChan
                            _ <- forkIO $ dataConsumer x c atomicPutStrLn
                                                       feedback runBH' idx map
                            return (feedback)) [1..nbWorkers]
  producer c threads atomicPutStrLn

  where
    producer c threads atomicPutStrLn = do

      mapM_ (writeChan c) [ x | x <- S.chunksOf 100 $ docs]
      mapM_ (writeChan c) [ [] | x <- threads]
      atomicPutStrLn "[main] eof sent, waiting for consumers to finish..."
      mapM_ (\x -> do let
                      threadRet <- readChan x
                      return threadRet) threads
      atomicPutStrLn "[main] all consumers end"

    dataConsumer id cs print feedback runBH' idx map = do
      someAds <- readChan cs
      case length $ someAds  of
        0 -> do
          print $ "thread #" ++ show(id) ++ " done";
          writeChan feedback True
        x -> do
             let bulkData = V.fromList [BulkIndex idx map (B.DocId (B.list_id (x :: B.BsearchDocs))) (toJSON x) | x <- someAds ]
             res <- runBH runBH' $ bulk bulkData
             let maybeResult = A.eitherDecode (responseBody res) :: Either String (BulkResult)
             case maybeResult of
               Left err -> print $ "thread #" ++ show(id) ++ " : "  ++  err ++ "\n" ++ show(responseBody res)
               Right resp -> print $ "thread #" ++ show(id) ++ " : "  ++  (if (errors resp) == False then "no errors" else "some errors") ++ " occurs while bulking " ++ show(length(items resp)) ++ " item(s)"
             dataConsumer id cs print feedback runBH' idx map


policy = simpleCorsResourcePolicy
           { corsRequestHeaders = [ "content-type" ] }

runServer :: Manager -> Poc -> IO ()
runServer manager servConf =  do
  W.run (port servConf) (cors (const $ Just policy)
                         $ provideOptions sameAPI
                         $ serve sameAPI (sameServer manager servConf))

sameAPI :: Proxy SameAPI
sameAPI = Proxy :: Proxy SameAPI

hotSwap runBH' esHost esUser esPasswd esIdx esAliasIdx esMap bsearchHost bsearchPort = do

  let idx = esIdx
  let aliasName = esAliasIdx
  let iAlias = IndexAlias idx (IndexAliasName aliasName)
  let aliasCreate = IndexAliasCreate Nothing Nothing
  respIsTwoHunna <- runBH' (createIndex defaultIndexSettings idx)
  respIsTwoHunna <- runBH' (updateIndexAliases (AddAlias iAlias aliasCreate L.:| []))
  print $ "Not yet fully implemented"

_esUser cmds = (if (esUser cmds) /= "" then esUser cmds else "elastic")
_esPasswd cmds = (if (esPasswd cmds) /= "" then esPasswd cmds else "changeme")
_esIdx cmds = IndexName $ pack $ esIdx cmds
_esAliasIdx cmds = IndexName $ pack $ esAliasIdx cmds
_esMap cmds = MappingName $ pack $ esMap cmds

sameServer :: Manager -> Poc -> S.Server SameAPI
sameServer manager conf =
  demo
  where
    demo id = liftIO $ sameGet id

    sameGet :: SameApiReq -> IO (Maybe SameApiResp)
    sameGet id = do
      print $ ">>> " ++ show(list_id id)
      let fields = L.nonEmpty ["body","subject" ]
      let like = L.nonEmpty [Same (_esIdx conf) (_esMap conf) (show(list_id id))]
      mRes <- moreLikeThisRequest manager (esHost conf) (_esUser conf) (_esPasswd conf)
              fields like
      let body = responseBody mRes
      let d = (A.eitherDecode body) :: (Either String (SearchResult B.BsearchDocs))

      case d of
        Left err -> do
          print err
          return $ Nothing
        Right ps -> do
          print $ "Elastic search found " ++ show(length $ hits $ searchHits $ ps)
            ++ " result(s)"
          let esRes = [unpackId(hitDocId x) | x <- (hits $ searchHits $ ps)]
          print esRes
          let listIds = intercalate "," $ [ unpack x | x <- esRes]
          let qs = "J0 lim:10 _cols:list_id,subject,body id:" ++ listIds ++ "\n"
          bRes <- B.bSearch (bsearchHost conf) (bsearchPort conf)
            $ "J0 lim:10 _cols:list_id,subject,body id:" ++ listIds ++ "\n"

--          print $ "bsearch returns : " ++ show bRes ++ "\n"
          return $ Just $ SameApiResp bRes

type SameAPI =
  "same_docs" :> ReqBody '[JSON] SameApiReq :> Post '[JSON] (Maybe SameApiResp)

data SameApiReq = SameApiReq {
  list_id :: Integer
  } deriving (Show, Generic)

data SameApiResp = SameApiResp {
  same_docs :: Maybe [B.Docs]
  } deriving (Show, Generic)

instance ToJSON SameApiResp
instance A.FromJSON SameApiReq


data Poc = Import { input :: String
                  , decode :: Bool
                  , es_host :: String
                  , es_user :: String
                  , es_passwd :: String
                  , es_idx :: String
                  , es_alias_idx :: String
                  , es_map :: String
                  }
  | Server {
      port :: Int
      , esHost :: String
      , esUser :: String
      , esPasswd :: String
      , esIdx :: String
      , esAliasIdx :: String
      , esMap :: String
      , bsearchHost :: String
      , bsearchPort :: String
      }
  | Export {
      input :: String
      , trans_host :: String
      , trans_port :: String
      , admin :: String
      , admin_passwd :: String
      }
  | HotSwap {
      es_host :: String
      , es_user :: String
      , es_passwd :: String
      , es_idx :: String
      , es_alias_idx :: String
      , es_map :: String
      , bsearch_host :: String
      , bsearch_port :: String
      }
  deriving (Show, Data, Typeable)

esHostFlags x = x &= name "es-host" &= explicit &= help "es uri (http://x.x.x.x:9200)" &= typ "URL"
esUserFlags x = x &= name "es-user" &= explicit &= help "es username (default elastic)"  &= typ "USERNAME"
esPasswdFlags x = x &= name "es-passwd" &= explicit &= help "es password (default changeme)" &= typ "PASSWORD"

esIdxFlags x = x &= name "es-idx" &= explicit &= help "es index to work on" &= typ "STRING"
esAliasIdxFlags x = x &= name "es-alias-idx" &= explicit &= help "es alias index to work on" &= typ "STRING"
esMapFlags x = x &= name "es-map" &= explicit &= help "es map to work on" &= typ "STRING"

_import = Import {
  input = def &= name "input" &= help "bsearch json data file" &= typ "FILE"
  , decode = def &= name "decode" &= help "decode latin9 input file and convert to utf8 before processing"
  , es_host = esHostFlags  ""
  , es_user = esUserFlags ""
  , es_passwd = esPasswdFlags ""
  , es_idx = esIdxFlags ""
  , es_alias_idx = esAliasIdxFlags ""
  , es_map = esMapFlags ""
  } &= help "import json data to elastic search"

_export = Export {
  input = def &= name "input" &= help "bsearch json data file" &= typ "FILE"
  , trans_host = def &= name "trans-host" &= help "trans host" &= typ "HOST"
  , trans_port = def &= name "trans-port" &= help "trans port" &= typ "PORT"
  , admin = def &= name "admin" &= help "admin username" &= typ "USERNAME"
  , admin_passwd = def &= name "admin-passwd" &= help "admin passwd" &= typ "PASSWORD"
  } &= help "export json data to trans/bdd"

_server = Main.Server {
  port = def &= name "port"  &= help "listen port" &= typ "PORT"
  , esHost = esHostFlags  ""
  , esUser = esUserFlags ""
  , esPasswd = esPasswdFlags ""
  , esIdx = esIdxFlags ""
  , esAliasIdx = esAliasIdxFlags ""
  , esMap = esMapFlags ""
  , bsearchHost = def &= help "bsearch host" &= typ "HOST"
  , bsearchPort = def &= help "bsearch port" &= typ "PORT"
  } &= help "serve document similarity api"

_hotswap = HotSwap {
  es_host = esHostFlags  ""
  , es_user = esUserFlags ""
  , es_passwd = esPasswdFlags ""
  , es_idx = esIdxFlags ""
  , es_alias_idx = esAliasIdxFlags ""
  , es_map = esMapFlags ""
  , bsearch_host = def &= help "bsearch host" &= typ "HOST"
  , bsearch_port = def &= help "bsearch port" &= typ "PORT"
  } &= help "index bsearch data and hotswap alias index"


main :: IO ()
main = do

  cmds <- cmdArgs $ modes [ _import, _export, _server, _hotswap ]
          &= help "manage elastic search poc : api server and utlities"

  let esUser = (if (es_user cmds) /= "" then es_user cmds else "elastic")
  let esPasswd = (if (es_passwd cmds) /= "" then es_passwd cmds else "changeme")
  let esIdx = IndexName $ pack $ es_idx cmds
  let esAliasIdx = IndexName $ pack $ es_alias_idx cmds
  let esMap = MappingName $ pack $ es_map cmds
  manager <- newManager defaultManagerSettings
  let runBH'' = (mkBHEnv (B.Server $ pack (es_host cmds)) manager)
                {
                  bhRequestHook = basicAuthHook
                                  (EsUsername $ pack esUser)
                                  (EsPassword $ pack esPasswd)
                }

  case cmds of
    Import {} -> do
      print "Reading input json"

      stream <- UF.readFile' (input cmds) (8192*1024)
      print $ decode cmds
      let inputStream  = (if (decode cmds) == True
                          then LE.encodeUtf8 $ LE.decodeLatin1 $ stream
                          else stream)
      importData inputStream

      where
        importData stream = do
          -- do not parse full file for two or more headers
          let header = take 1 $
                parseLazyByteString (objectWithKey "info" value) $ stream :: [B.BsearchInfo]
          print header

          let docs = parseLazyByteString (objectWithKey "docs" (arrayOf value)) $ stream :: [B.BsearchDocs]

          jsonHandle docs runBH'' esIdx esAliasIdx esMap
          print "Done"

    Export input transHost transPort admin adminPasswd -> do
      print "not yet implemented"

    Main.Server {} -> do
      print $ "Server listening on port " ++ show (port cmds)
      runServer manager cmds

    HotSwap esHost _ _ _ _ _ bsearchHost bsearchPort -> do
      hotSwap (runBH runBH'') esHost esUser esPasswd esIdx esAliasIdx esMap bsearchHost bsearchPort
