module Main where

import Control.Monad
import Data.Ix

import Distributed

type Msg = String

data Adr = P1 | P2
    deriving (Bounded, Enum, Eq, Ord, Ix, Show)

example :: System Adr Msg IO ()
example P1 = forever $ do
    send P2 "cat"
    (adr, msg) <- recv
    internal (putStrLn $ show adr ++ msg)
example P2 = forever $ do
    internal . print =<< time
    (adr, msg) <- recv
    internal . print =<< time
    send P1 (msg ++ "fish")

main :: IO ()
main = roundRobinLamport example
-- main = threadedLamport example