{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}

import           Data.Aeson             (FromJSON (..), defaultOptions,
                                         genericParseJSON, genericToJSON,
                                         object, (.=), eitherDecode)
import           Data.Text              (Text)
import           Data.List.Split

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

      mapM_ (writeChan c) [ x | x <- (chunksOf 20 (take 200 $ (docs adData)))]
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
             res <- mapM (\x -> do let
                                   response <- runBH runBH' $ indexDocument testIndex testMapping defaultIndexDocumentSettings x (DocId (list_id x))
                                   return (list_id x, statusMessage $ responseStatus $ response)) someAds
             print $ "thread #" ++ show(id) ++ " : "  ++ show res
             dataConsumer id cs print feedback runBH'


main :: IO ()
main = do
  let testServer = Server "http://172.17.0.2:9200"

  manager <- newManager defaultManagerSettings

  let runBH'' = (mkBHEnv testServer manager) {bhRequestHook = basicAuthHook (EsUsername "elastic") (EsPassword "changeme")}

{-
  let query = TermQuery (Term "body" "piano") Nothing
  let search = mkSearch (Just query) Nothing
  reply <- runBH runBH'' $ searchAll search
  print reply
-}
  
  print "Reading input json"
  
  d <- (eitherDecode <$> getJSON) :: IO (Either String Bsearch)

  case d of
    Left err -> putStrLn err
    Right ps -> json_handle ps runBH''
