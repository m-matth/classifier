{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE NoMonomorphismRestriction #-} -- toHtmlRaw
{-# LANGUAGE MultiParamTypeClasses #-} -- RawHtml

module Main where

import Data.Char
import Data.List
import Data.Function (on)
import Data.Text (Text)
import Data.Proxy (Proxy(..))
import Data.Aeson (ToJSON, toJSON, object, (.=), FromJSON, parseJSON, (.:), withObject, Object)
import GHC.Generics (Generic)
import qualified Data.Text.Lazy as T

import System.IO
import Numeric

import Servant.API
import Servant.Server
import Servant.HTML.Lucid
import Lucid
import Network.HTTP.Media  ((//), (/:))
import Network.Wai.Handler.Warp (run)

-- Hext
import NLP.Hext.NaiveBayes
import qualified Data.HashMap.Lazy as H
import qualified Data.Set as S


autoFill [] foo = []
autoFill (x:xs) foo = (autoFill xs foo) ++ lookup
  where
    best_val = head $ (foo `intersect` words x ) ++ [ "unknown" ]
    lookup = [(x, best_val)] 

setup :: IO (ServConf)
setup = do
      stopwords_file <- readFile "stopwords.txt"
      let stopwords = sort(lines stopwords_file)
      putStrLn $ "\t * Loaded " ++ show(length(stopwords)) ++ " stopwords\n"

      label_file <- readFile "top42_label"
      let labels = take 550 $ (lines label_file) \\ stopwords
      putStrLn $ "\t * Loaded "
        ++ show(length(labels))
        ++ " valid labels, filtered out "
        ++ show(length(take 250 $ lines label_file) - length(labels))
        ++ " stopwords\n"

      body_file <- readFile "ad_body.txt"

      let sample_count = 500
      let samples = take sample_count $ (lines (body_file))
      let body = take 10 $ samples

      putStrLn $ "\t * AutoLabel " ++ show(sample_count) ++ " samples\n"
      let !autoLabeled = autoFill samples labels
      let !model = [ (x, y) | (x, y) <- autoLabeled, length(y) /= 0 ]

      let teachModel = foldl (\md (sample, cl) -> teach (T.pack sample) cl md) emptyModel
      putStrLn $ "\t * Teach model with autolabeled collection"
      let !model = teachModel autoLabeled
      putStrLn $ "\t * model ready !"

      html_file <- readFile "www/homepage.html"

      let conf = ServConf model html_file labels stopwords
      return conf

 
main :: IO()
main = do
  putStrLn "Compute dummy naive bayes ..."
  conf <- setup
 
  putStrLn "..Done"
  runServer conf


data ServConf = ServConf {
  model :: BayesModel String,
  demoPage :: String,
  labels :: [String],
  stopword :: [String]
  }

data Ad = Ad { subject :: String
             , body :: String
             } deriving (Show, Generic)

data Label = Label
  { tag :: String,
    score :: String ,
    score2 :: Double
  } deriving (Show, Generic)

data Page = Page {
  content :: String
  } deriving (Show, Generic)

instance ToJSON Label
instance ToJSON Ad

instance FromJSON Label
instance FromJSON Ad

instance ToHtml Page where
  toHtml page = toHtmlRaw $ content page

getLabelHandler :: ServConf -> Ad -> Handler Label
getLabelHandler conf ad = do
  let label = runBayesWithScore (model conf) (body ad)
  return (Label (fst label) (take 10 $ showFFloat Nothing (snd label) "") (snd label))

demo :: ServConf -> Handler Page
demo conf = do
    return $ Page $ demoPage conf

labelsAPI :: Proxy LabelsAPI
labelsAPI = Proxy :: Proxy LabelsAPI


labelsServer :: ServConf -> Server LabelsAPI
labelsServer conf = 
  (getLabelHandler conf)  :<|>
  (demo conf)

runServer :: ServConf -> IO ()
runServer conf = do
  run 8000 (serve labelsAPI (labelsServer conf))



type LabelsAPI =
  "labels" :> ReqBody '[JSON] Ad :> Post '[JSON] Label :<|>
  "index" :> Get '[HTML] Page



--
-- try compute score using prob field from NLP.Hext.NaiveBayes
--

runBayesWithScore model sample = r where
  max = S.findMax $ classify model (T.words $ T.pack sample)
  r = (_class max, ((probability max)))

runBayesWithAllScore model sample = S.elems (classify model (T.words $ T.pack sample))

classify model = f where
    cs = classes model
    lengthVocab = H.size $ vocab model
    mat = material model
    prob c ws = 
        let caseC = unions . vecs $ filter ((== c) . label) mat
            n = totalWords caseC
            denom = n + lengthVocab
        in foldl' (\acc word -> (pWordGivenClass word denom caseC) * acc) (pClass c mat) ws
    f wrds = S.map (\c -> Classified c $ prob c wrds) cs

-- the probability of a class occurs,
-- given a set of learning material
pClass :: (Eq a) => a -> [Labeled a] -> Double
pClass cl [] = 0
pClass cl docs =
    let count = length $ filter (\(Labeled fl clas) -> clas == cl) docs
    in (fromIntegral count) / (fromIntegral $ length docs)

-- the probability the word occurs given the class
pWordGivenClass :: T.Text -> Int -> FrequencyList -> Double
pWordGivenClass w denom currentCase =
    (fromIntegral (nk + 1)) / (fromIntegral denom) where
        nk = totalOfWord w currentCase    

removePunctuation :: T.Text -> T.Text
removePunctuation = T.filter (not . isPunctuation)

-- a list of frequency lists, derived from a set of material
vecs :: [Labeled a] -> [FrequencyList]
vecs = map hash

-- the union of multiple frequency lists
-- adds occurences of each word together
unions :: [FrequencyList] -> FrequencyList
unions = foldl' (\acc hmap -> H.unionWith (+) hmap acc) H.empty 

totalWords :: FrequencyList -> Int
totalWords = H.foldl' (+) 0 

totalOfWord :: T.Text -> FrequencyList -> Int
totalOfWord word doc = H.lookupDefault 0 word doc

