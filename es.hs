{-# LANGUAGE DeriveGeneric     #-}
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

import Database.V5.Bloodhound (bhRequestHook, SearchResult(..), runBH, bulk,
                              mkBHEnv, basicAuthHook, EsUsername(..), EsPassword(..),
                              IndexName(..), MappingName(..), BulkOperation(..),
                              hits, unpackId, hitDocId, DocId(..), Server(..),
                              ToJSON(..))

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


jsonFile :: FilePath
jsonFile = "ad4.json"

getJSON :: IO BL.ByteString
getJSON = BL.readFile jsonFile

testIndex = IndexName "test"
testMapping = MappingName "Ad"
nbWorkers = 10

json_handle adData runBH' = do
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

runServer :: Manager -> IO ()
runServer manager = do
  W.run 8000 (cors (const $ Just policy) $ provideOptions sameAPI  $ serve sameAPI (sameServer manager))

sameAPI :: Proxy SameAPI
sameAPI = Proxy :: Proxy SameAPI


sameServer :: Manager -> S.Server SameAPI
sameServer manager =
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
          let qs = "J0 lim:10 _cols:list_id,subject,body id:" ++ show(intercalate "," $ [ unpack x | x <- esRes]) ++ "\n"
          bRes <- bSearch "www.jenkins.vdjkslave01.dev.leboncoin.lan" "20010" qs
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

esServer = Server "http://172.17.0.2:9200"

main :: IO ()
main = do
  manager <- newManager defaultManagerSettings

  let runBH'' = (mkBHEnv esServer manager) {bhRequestHook = basicAuthHook (EsUsername "elastic") (EsPassword "changeme")}

  
  print "Reading input json"
  
  d <- (A.eitherDecode <$> getJSON) :: IO (Either String Bsearch)
{-
  case d of
    Left err -> putStrLn err
    Right ps -> json_handle ps runBH''
-}

  runServer manager
