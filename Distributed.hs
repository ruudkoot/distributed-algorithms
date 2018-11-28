{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE LambdaCase                #-}
{-# LANGUAGE Rank2Types                #-}
{-# LANGUAGE ScopedTypeVariables       #-}

module Distributed where

import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Free
import Control.Monad.Trans.State

-- | Process monad

type Clk = Int

data ProcessF msg m next
    = forall a. Tau (m a) (a -> next)
    | Send msg next
    | Recv (msg -> next)
    | Time (Clk -> next)

instance Functor (ProcessF msg m) where
    fmap f (Tau action next) = Tau action (f . next)
    fmap f (Send msg next) = Send msg (f next)
    fmap f (Recv next) = Recv (f . next)
    fmap f (Time next) = Time (f . next)

type ProcessT msg m a = FreeT (ProcessF msg m) m a

internal :: (Monad m) => m a -> ProcessT msg m a
internal action = liftF (Tau action id)

send :: (Monad m) => msg -> ProcessT msg m ()
send msg = liftF (Send msg ())

recv :: (Monad m) => ProcessT msg m msg
recv = liftF (Recv id)

time :: (Monad m) => ProcessT msg m Clk
time = liftF (Time id)

-- * Schedulers

roundRobin :: (Monad m) => [(String, ProcessT msg m a)] -> m ()
roundRobin = roundRobin' []

roundRobin' :: (Monad m) => [msg] -> [(String, ProcessT msg m a)] -> m ()
roundRobin' mq [] = return ()
roundRobin' mq ((lbl, p) : pq) = runFreeT p >>= \case
    Free (Tau action next) -> do
        x <- action
        roundRobin' mq (pq ++ [(lbl, next x)])
    Free (Send msg next) -> do
        roundRobin' (mq ++ [msg]) (pq ++ [(lbl, next)])
    Free (Recv next) -> do
        case mq of
            [] -> do
                roundRobin' mq (pq ++ [(lbl, p)])
            (msg : mq) -> do
                roundRobin' mq (pq ++ [(lbl, next msg)])
    Free (Time next) -> do
        roundRobin' mq (pq ++ [(lbl, next 0)])
    Pure x -> do
        roundRobin' mq pq

-- | Example processes

type Msg = String

process1, process2 :: ProcessT Msg IO ()
process1 = forever $ do
    send "cat"
    msg <- recv
    internal (putStrLn msg)
process2 = forever $ do
    msg <- recv
    send (msg ++ "fish")
