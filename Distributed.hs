-- TODO: Basic vs control message: snapshots

{-# LANGUAGE ConstraintKinds           #-}
{-# LANGUAGE EmptyCase                 #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE LambdaCase                #-}
{-# LANGUAGE PatternSynonyms           #-}
{-# LANGUAGE Rank2Types                #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TupleSections             #-}
{-# LANGUAGE ViewPatterns              #-}

module Distributed (
    Clk,
    System,
    internal,
    send,
    recv,
    time,
    roundRobinLamport,
    threadedLamport
) where

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

enumerate :: forall t. (Address t) => [t]   -- TODO: ugly function
enumerate = range (minBound :: t, maxBound)

-- | Process monad

-- TODO: Clk type differ between Lamport, Vector, Maxtrix clocks
type Clk = Int

-- TODO: datatypes-a-la-carte, trees-that-grow
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

type ProcessT adr msg m a = FreeT (ProcessF adr msg m) (StateT Clk m) a
type System adr msg m a = adr -> ProcessT adr msg m a

-- TODO: instance MonadProcess
internal :: (Monad m) => m a -> ProcessT adr msg m a
internal action = liftF (Tau action id)

send :: (Monad m) => adr -> msg -> ProcessT adr msg m ()
send adr msg = liftF (Send adr msg ())

recv :: (Monad m) => ProcessT adr msg m (adr, msg)
recv = liftF (Recv id)

time :: (Monad m) => ProcessT adr msg m Clk
time = liftF (Time id)

-- | Schedulers

-- * Round-robin
-- TODO: Fair stochastic scheduler

roundRobinLamport :: (Address adr, Monad m) => System adr msg m a -> m ()
roundRobinLamport system =
    let adrs = enumerate
        mq = Map.fromDistinctAscList (map (, Seq.empty) adrs)
        pq = Seq.fromList (map (\adr -> (adr, 0, system adr)) adrs)
     in fst <$> runStateT (roundRobinL' mq pq) (error "clk")

type MsgQ adr msg = Map adr (Seq (adr, Clk, msg))
type ProcessQ adr msg m a = Seq (adr, Clk, ProcessT adr msg m a)

-- TODO: detect deadlock due to no msgs
roundRobinL'
    :: (Address adr, Monad m)                   -- v- not making use of it..!
    => MsgQ adr msg -> ProcessQ adr msg m a -> StateT Clk m ()
roundRobinL' mq (viewl -> EmptyL) = return ()

roundRobinL' mq (viewl -> (self, clk, p) :< pq) = runFreeT p >>= \case
    Free (Tau action next) -> do
        x <- lift action
        roundRobinL' mq (pq |> (self, clk + 1, next x))
    Free (Send adr msg next) ->
        roundRobinL'
            (Map.adjust (|> (self, clk, msg)) adr mq)
            (pq |> (self, clk + 1, next))
    Free (Recv next) -> case viewl (mq ! self) of
        EmptyL ->
            roundRobinL' mq (pq |> (self, clk, p))
        ((adr, clk', msg) :< msgs) ->
            roundRobinL'
                (Map.insert self msgs mq)
                (pq |> (self, max clk clk' + 1, next (adr, msg)))
    Free (Time next) ->
        roundRobinL' mq (pq |> (self, clk, next clk))
    Pure x ->
        roundRobinL' mq pq

-- * Threaded
-- TODO: Process/machine parallel scheduler

type Threaded adr msg a =
    adr -> (adr -> Chan (adr, Clk, msg)) ->
        ProcessT adr msg IO a -> StateT Clk IO a

threaded :: (Address adr) => Threaded adr msg a -> System adr msg IO a -> IO ()
threaded interpreter system = do
    -- TODO: gather return values of threads
    let adrs = enumerate
    cs <- mapM (const newChan) adrs
    forM_ adrs $ \adr -> forkIO . void . flip runStateT 0 $
        interpreter adr (\adr -> cs !! fromEnum adr) (system adr)

threadedL' :: Threaded adr msg a
threadedL' self cs p = runFreeT p >>= \case
    -- TODO: pattern synonyms / eliminator
    Free (Tau action next) -> do
        modify succ
        x <- lift action
        threadedL' self cs (next x)
    Free (Send adr msg next) -> do
        clk <- get
        lift (writeChan (cs adr) (self, clk, msg))
        modify succ
        threadedL' self cs next
    Free (Recv next) -> do
        (adr, clk, msg) <- lift (readChan (cs self))
        modify (max clk)
        threadedL' self cs (next (adr, msg))
    Free (Time next) -> do
        clk <- get
        threadedL' self cs (next clk)
    Pure x ->
        return x

threadedLamport :: (Address adr) => System adr msg IO a -> IO ()
threadedLamport = threaded threadedL'

-- | References

-- * Concurrency monads
-- http://www.haskellforall.com/2013/06/from-zero-to-cooperative-threads-in-33.html

-- * Free monads
-- https://softwareengineering.stackexchange.com/questions/242795/what-is-the-free-monad-interpreter-pattern
-- https://www.tweag.io/posts/2018-02-05-free-monads.html
-- https://markkarpov.com/post/free-monad-considered-harmful.html