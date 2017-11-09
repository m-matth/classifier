{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}

import           Data.Aeson             (FromJSON (..), defaultOptions,
                                         genericParseJSON, genericToJSON,
                                         object, (.=), eitherDecode, encode,
                                         Value(..))
import           Data.Text              (Text)
import           Data.List.Split
import qualified Data.Vector            as V
import qualified Data.ByteString.Lazy   as B
import           Database.V5.Bloodhound
import           GHC.Generics           (Generic)

import           Network.HTTP.Client (defaultManagerSettings)
import           Network.HTTP.Client.Internal
import           Network.HTTP.Types.Status

import           Control.Concurrent
import           Control.Concurrent.Chan

import qualified Data.List.NonEmpty     as L

import           Lib.Utils

data Conf = Conf {
    adData :: Bsearch
  , elasticSearch :: BHEnv
  }
          
jsonFile :: FilePath
jsonFile = "ad.json"

getJSON :: IO B.ByteString
getJSON = B.readFile jsonFile

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

      mapM_ (writeChan c) [ x | x <- (chunksOf 50 (take 1000 $ (docs adData)))]
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
             let bulkData = V.fromList [BulkIndex testIndex testMapping (DocId (list_id x)) (toJSON x) | x <- someAds ]
             res <- runBH runBH' $ bulk bulkData
             let maybeResult = eitherDecode (responseBody res) :: Either String (BulkResult)
             case maybeResult of
               Left err -> print $ "thread #" ++ show(id) ++ " : "  ++  err ++ "\n" ++ show(responseBody res)
               Right resp -> print $ "thread #" ++ show(id) ++ " : "  ++  (if (errors resp) == False then "no errors" else "some errors") ++ " occurs while bulking " ++ show(length(items resp)) ++ " item(s)"
             dataConsumer id cs print feedback runBH'



main :: IO ()
main = do
  let testServer = Server "http://172.17.0.2:9200"

  manager <- newManager defaultManagerSettings

  let runBH'' = (mkBHEnv testServer manager) {bhRequestHook = basicAuthHook (EsUsername "elastic") (EsPassword "changeme")}

  
  print "Reading input json"
  
  d <- (eitherDecode <$> getJSON) :: IO (Either String Bsearch)

  case d of
    Left err -> putStrLn err
    Right ps -> json_handle ps runBH''

  let fields = L.nonEmpty ["body","subject" ]
  let like = L.nonEmpty [Same testIndex "Ad" "1121403928"]

  response <- moreLikeThisRequest manager "elastic" "changeme" fields like
  let body = responseBody response
  let d = (eitherDecode body) :: (Either String (SearchResult BsearchDocs))

  case d of
    Left err -> print err
    Right ps -> do
      print $ "found " ++ show (length(hits $ searchHits $ ps)) ++ " result(s)"
      print [unpackId(hitDocId x) | x <- (hits $ searchHits $ ps)]
