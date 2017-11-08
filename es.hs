{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DuplicateRecordFields #-}

import           Data.Aeson             (FromJSON (..), defaultOptions,
                                         genericParseJSON, genericToJSON,
                                         object, (.=), eitherDecode, encode,
                                         Value(..))
import           Data.Text              (Text)
import           Data.List.Split
import qualified Data.List.NonEmpty     as L
import qualified Data.Vector            as V
import qualified Data.ByteString.Lazy   as B
import           Database.V5.Bloodhound
import           GHC.Generics           (Generic)

import           Network.HTTP.Client (defaultManagerSettings)
import           Network.HTTP.Client.Internal
import           Network.HTTP.Types.Status

import           Control.Concurrent
import           Control.Concurrent.Chan


data BsearchInfo = BsearchInfo
  {
    lines :: Integer
  , to_id :: String
  , filtered :: Integer
  } deriving (Show, Generic)

data BsearchDocs = BsearchDocs
  {
    list_id :: Text
  , subject :: Maybe String
  , body :: Maybe String
  } deriving (Show, Generic)

data Bsearch = Bsearch
 {
   info :: Maybe BsearchInfo
 , docs :: [BsearchDocs]
 } deriving (Show, Generic)

instance FromJSON BsearchInfo where
  parseJSON = genericParseJSON defaultOptions

instance ToJSON BsearchDocs where
  toJSON = genericToJSON defaultOptions
instance FromJSON BsearchDocs where
  parseJSON = genericParseJSON defaultOptions

instance FromJSON Bsearch where
  parseJSON = genericParseJSON defaultOptions

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

data Job = Job {
    ads :: [BsearchDocs]
  , eof :: Bool
  } deriving(Show)


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

data BulkIndexItem = BulkIndexItem {
  index :: BulkItem
  } deriving (Eq, Read, Show, Generic)

instance FromJSON BulkIndexItem where
  parseJSON = genericParseJSON defaultOptions

data BulkResult = BulkResult {
  took  :: Int
  , errors :: Bool
  , items :: [BulkIndexItem]
  } deriving (Eq, Read, Show, Generic)

instance FromJSON BulkResult where
  parseJSON = genericParseJSON defaultOptions
 
data Same = Same {
  _index :: IndexName
  , _type :: [Char]
  ,_id :: [Char]
  } deriving (Eq, Read, Show, Generic)

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

moreLikeThisRequest manager = do
  -- Create the request
  let fields = L.nonEmpty ["body","subject" ]
  let like = L.nonEmpty [Same testIndex "Ad" "1121403928"]
  let requestObject = object [ "query" .= object [ "more_like_this" .= MoreLikeThisObj fields like 2 12 ] ]

  initialRequest <- applyBasicAuth "elastic" "changeme" <$> parseRequest "http://172.17.0.2:9200/_search"
  let request = initialRequest { method = "POST", requestBody = RequestBodyLBS $ encode requestObject }

  response <- httpLbs request manager
{-  putStrLn $ "The status code was: " ++ (show $ statusCode $ responseStatus response)
  print $ responseBody response -}
  return response

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

  response <- moreLikeThisRequest manager
  let body = responseBody response
  let d = (eitherDecode body) :: (Either String (SearchResult BsearchDocs))

  case d of
    Left err -> print err
    Right ps -> do
      print $ "found " ++ show (length(hits $ searchHits $ ps)) ++ " result(s)"
      print [unpackId(hitDocId x) | x <- (hits $ searchHits $ ps)]
