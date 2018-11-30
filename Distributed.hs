{-# LANGUAGE ConstraintKinds           #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE LambdaCase                #-}
{-# LANGUAGE PatternSynonyms           #-}
{-# LANGUAGE Rank2Types                #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TupleSections             #-}
{-# LANGUAGE ViewPatterns              #-}

module Distributed where

import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Free
import Control.Monad.Trans.State

import Control.Concurrent
import Control.Concurrent.Chan

import Data.Array hiding ((!))
import Data.Ix
import Data.Map (Map, (!))
import qualified Data.Map as Map
import Data.Sequence (Seq(..), ViewL(..), (|>), viewl)
import qualified Data.Sequence as Seq

-- | Prelude

type Address t = (Bounded t, Enum t, Ix t, Ord t)

enumerate :: forall t. (Address t) => [t]
enumerate = range (minBound :: t, maxBound)

-- | Process monad

type Clk = Int

data ProcessF adr msg m next
    = forall a. Tau (m a) (a -> next)
    | Send adr msg next
    | Recv ((adr, msg) -> next)
    | Time (Clk -> next)

instance Functor (ProcessF adr msg m) where
    fmap f (Tau action next) = Tau action (f . next)
    fmap f (Send adr msg next) = Send adr msg (f next)
    fmap f (Recv next) = Recv (f . next)
    fmap f (Time next) = Time (f . next)

type ProcessT adr msg m a = FreeT (ProcessF adr msg m) m a

type System adr msg m a = adr -> ProcessT adr msg m a

internal :: (Monad m) => m a -> ProcessT adr msg m a
internal action = liftF (Tau action id)

send :: (Monad m) => adr -> msg -> ProcessT adr msg m ()
send adr msg = liftF (Send adr msg ())

recv :: (Monad m) => ProcessT adr msg m (adr, msg)
recv = liftF (Recv id)

time :: (Monad m) => ProcessT adr msg m Clk
time = liftF (Time id)

-- | Schedulers

-- * Round-Robin (sequential)

roundRobin :: (Address adr, Monad m) => System adr msg m a -> m ()
roundRobin system =
    let adrs = enumerate
        mq = Map.fromDistinctAscList (map (, Seq.empty) adrs) 
        pq = Seq.fromList (map (\adr -> (adr, system adr)) adrs)
     in roundRobin' mq pq

roundRobin' :: (Address adr, Monad m) => Map adr (Seq (adr, msg)) -> Seq (adr, ProcessT adr msg m a) -> m ()
roundRobin' mq (viewl -> EmptyL) = return ()
roundRobin' mq (viewl -> (self, p) :< pq) = runFreeT p >>= \case
    Free (Tau action next) -> do
        x <- action
        roundRobin' mq (pq |> (self, next x))
    Free (Send adr msg next) -> do
        roundRobin' (Map.adjust (|> (self, msg)) adr mq) (pq |> (self, next))
    Free (Recv next) -> do
        case viewl (mq ! self) of
            EmptyL -> do
                roundRobin' mq (pq |> (self, p))
            (msg :< msgs) -> do
                roundRobin' (Map.insert self msgs mq) (pq |> (self, next msg))
    Free (Time next) -> do
        roundRobin' mq (pq |> (self, next 0))
    Pure x -> do
        roundRobin' mq pq

-- * Threaded (parallel)

threaded :: (Address adr) => System adr msg IO a -> IO ()
threaded system = do
    let adrs = enumerate
    cs <- mapM (const newChan) adrs
    forM_ adrs $
        \adr -> forkIO (threaded' adr (\adr -> cs !! fromEnum adr) (system adr))

threaded' :: adr -> (adr -> Chan (adr, msg)) -> ProcessT adr msg IO a -> IO ()
threaded' self cs p = runFreeT p >>= \case
    Free (Tau action next) -> do
        x <- action
        threaded' self cs (next x)
    Free (Send adr msg next) -> do
        writeChan (cs adr) (self, msg)
        threaded' self cs next
    Free (Recv next) -> do
        (adr, msg) <- readChan (cs self)
        threaded' self cs (next (adr, msg))
    Free (Time next) -> do
        threaded' self cs (next 0)
    Pure x -> do
        return ()

-- | Examples

type Msg = String

data Adr = P1 | P2
    deriving (Bounded, Enum, Eq, Ord, Ix, Show)

example :: System Adr Msg IO ()
example P1 = forever $ do
    send P2 "cat"
    (adr, msg) <- recv
    internal (putStrLn $ show adr ++ msg)
example P2 = forever $ do
    (adr, msg) <- recv
    send P1 (msg ++ "fish")