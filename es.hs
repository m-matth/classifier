{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}

import GHC.Generics (Generic)
import Control.Monad.IO.Class

import qualified Data.Aeson             as A (FromJSON (..), defaultOptions,
                                         genericParseJSON, genericToJSON,
                                         object, (.=), eitherDecode, encode,
                                         toJSON, Value(..))
import Data.Text              (Text, pack, unpack)
import Data.List.Split as S
import Data.List (intercalate)
import Data.Proxy (Proxy(..))

import qualified Data.Vector as V
import qualified Data.ByteString.Lazy as BL

import Database.V5.Bloodhound as B (Server(..), ToJSON(..), SearchResult(..),
                                    bulk, hitDocId, unpackId, hits,
                                    BulkOperation(..), MappingName(..),
                                    IndexName(..), DocId(..), runBH,
                                    bhRequestHook, mkBHEnv, basicAuthHook,
                                    EsUsername(..), EsPassword(..))

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

import Lib.Utils
import Lib.Utils.Blocket (bSearch, Docs(..)
                         , BsearchDocs(..), Bsearch(..))
import Lib.Utils.Bloodhound (moreLikeThisRequest, BulkResult(..),
                            Same(..))

import System.Console.CmdArgs

jsonFile :: FilePath
jsonFile = "ad4.json"

getJSON :: IO BL.ByteString
getJSON = BL.readFile jsonFile

--testIndex = IndexName "test"
testIndex = IndexName "index2"
testMapping = MappingName "Ad"
nbWorkers = 10

jsonHandle adData runBH' = do
  lock <- newMVar ()
  let atomicPutStrLn str = withMVar lock (\_ -> putStrLn str)
  atomicPutStrLn $ "Launch " ++ show(nbWorkers)++ " to handle data"

  c  <- newChan

  threads <- mapM (\x -> do let
                            feedback <- newChan
                            _ <- forkIO $ dataConsumer x c atomicPutStrLn feedback runBH'
                            return (feedback)) [1..nbWorkers]
  producer c threads atomicPutStrLn

  where
    producer c threads atomicPutStrLn = do

      mapM_ (writeChan c) [ x | x <- (S.chunksOf 100 (take 1000000 $ (docs adData)))]
      mapM_ (writeChan c) [ [] | x <- threads]
      atomicPutStrLn "[main] eof sent, waiting for consumers to finish..."
      mapM_ (\x -> do let
                      threadRet <- readChan x
                      return threadRet) threads
      atomicPutStrLn "[main] all consumers end"

    dataConsumer id cs print feedback runBH' = do
      someAds <- readChan cs
      case length $ someAds  of
        0 -> do
          print $ "thread #" ++ show(id) ++ " done";
          writeChan feedback True
        x -> do
             let bulkData = V.fromList [BulkIndex testIndex testMapping (DocId (list_id (x :: BsearchDocs))) (toJSON x) | x <- someAds ]
             res <- runBH runBH' $ bulk bulkData
             let maybeResult = A.eitherDecode (responseBody res) :: Either String (BulkResult)
             case maybeResult of
               Left err -> print $ "thread #" ++ show(id) ++ " : "  ++  err ++ "\n" ++ show(responseBody res)
               Right resp -> print $ "thread #" ++ show(id) ++ " : "  ++  (if (errors resp) == False then "no errors" else "some errors") ++ " occurs while bulking " ++ show(length(items resp)) ++ " item(s)"
             dataConsumer id cs print feedback runBH'


policy = simpleCorsResourcePolicy
           { corsRequestHeaders = [ "content-type" ] }

runServer :: Manager -> Int -> String -> String -> IO ()
runServer manager port bsearchHost bsearchPort = do
  W.run port (cors (const $ Just policy) $ provideOptions sameAPI  $ serve sameAPI (sameServer manager bsearchHost bsearchPort))

sameAPI :: Proxy SameAPI
sameAPI = Proxy :: Proxy SameAPI


sameServer :: Manager -> String -> String -> S.Server SameAPI
sameServer manager bsearchHost bsearchPort =
  demo
  where
    demo id    = liftIO $ sameGet id

    sameGet :: SameApiReq -> IO (Maybe SameApiResp)
    sameGet id = do
      print $ ">>> " ++ show(list_id (id::SameApiReq))
      let fields = L.nonEmpty ["body","subject" ]
      let like = L.nonEmpty [Same testIndex "Ad" (show(list_id (id::SameApiReq)))]
      mRes <- moreLikeThisRequest manager "elastic" "changeme" fields like
      let body = responseBody mRes
      let d = (A.eitherDecode body) :: (Either String (SearchResult BsearchDocs))

      case d of
        Left err -> do
          print err
          return $ Nothing
        Right ps -> do
          print $ "Elastic search found " ++ show (length(hits $ searchHits $ ps)) ++ " result(s)"
          let esRes = [unpackId(hitDocId x) | x <- (hits $ searchHits $ ps)]
          print esRes
          let listIds = intercalate "," $ [ unpack x | x <- esRes]
          let qs = "J0 lim:10 _cols:list_id,subject,body id:" ++ listIds ++ "\n"
          bRes <- bSearch bsearchHost bsearchPort qs
--          bRes <- bSearch "www.jenkins.vdjkslave01.dev.leboncoin.lan" "20010" qs
--          print $ "bsearch returns : " ++ show bRes ++ "\n"
          return $ Just $ SameApiResp bRes

type SameAPI =
  "same_docs" :> ReqBody '[JSON] SameApiReq :> Post '[JSON] (Maybe SameApiResp)

data SameApiReq = SameApiReq {
  list_id :: Integer
  } deriving (Show, Generic)

data SameApiResp = SameApiResp {
  same_docs :: Maybe [Docs]
  } deriving (Show, Generic)

instance ToJSON SameApiResp
instance A.FromJSON SameApiReq


data Poc = Import { input :: String
                  , es_host :: String
                  , es_user :: String
                  , es_passwd :: String
                  }
  | Server {
      port :: Int
      , es_host :: String
      , es_user :: String
      , es_passwd :: String
      , bsearch_host :: String
      , bsearch_port :: String
      }
  deriving (Show, Data, Typeable)

esHostFlags x = x &= name "es-host" &= explicit &= help "es uri (http://x.x.x.x:9200)" &= typ "URL"
esUserFlags x = x &= name "es-user" &= explicit &= help "es username (default elastic)"  &= typ "USERNAME"
esPasswdFlags x = x &= name "es-passwd" &= explicit &= help "es password (default changeme)" &= typ "PASSWORD"

_import = Import {
  input = def &= name "input" &= help "bsearch json data file" &= typ "FILE"
  , es_host = esHostFlags  ""
  , es_user = esUserFlags ""
  , es_passwd = esPasswdFlags ""
  }  &= help "import json data to elastic search"

_server = Main.Server {
  port = def &= name "port"  &= help "listen port" &= typ "PORT"
  , es_host = esHostFlags  ""
  , es_user = esUserFlags ""
  , es_passwd = esPasswdFlags ""
  , bsearch_host = def &= help "bsearch host" &= typ "HOST"
  , bsearch_port = def &= help "bsearch port" &= typ "PORT"
  } &= help "serve document similarity api"

main :: IO ()
main = do

  cmds <- cmdArgs $ modes [ _import, _server]

  let esUser = (if (es_user cmds) /= "" then pack (es_user cmds) else "elastic")
  let esPasswd = (if (es_passwd cmds) /= "" then pack (es_passwd cmds) else "changeme1")

  manager <- newManager defaultManagerSettings

  case cmds of
    Import input esHost _ _ -> do
      print $ "Import " ++ show input
      -- esServer = B.Server "http://172.17.0.2:9200"
      let runBH'' = (mkBHEnv (B.Server $ pack esHost) manager)
            {
              bhRequestHook = basicAuthHook (EsUsername esUser) (EsPassword esPasswd)
            }
      print "Reading input json"
      d <- (A.eitherDecode <$> BL.readFile input) :: IO (Either String Bsearch)
      case d of
        Left err -> putStrLn err
        Right ps -> jsonHandle ps runBH''
      print "Done"

    Main.Server listen esHost user passwd bsearchHost bsearchPort -> do
      print $ "Server :" ++ show listen
      runServer manager listen bsearchHost bsearchPort

