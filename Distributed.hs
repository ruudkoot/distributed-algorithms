{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE LambdaCase                #-}
{-# LANGUAGE Rank2Types                #-}
{-# LANGUAGE ScopedTypeVariables       #-}

module Distributed where

import Control.Monad
import Control.Monad.Trans.Free

-- | Process monad

data ProcessF msg m next
    = forall a. Tau (m a) (a -> next)
    | Send msg next
    | Recv (msg -> next)

instance Functor (ProcessF msg m) where
    fmap f (Tau action next) = Tau action (f . next)
    fmap f (Send msg next) = Send msg (f next)
    fmap f (Recv next) = Recv (f . next)

type ProcessT msg m a = FreeT (ProcessF msg m) m a

internal :: (Monad m) => m a -> ProcessT msg m a
internal action = liftF (Tau action id)

send :: (Monad m) => msg -> ProcessT msg m ()
send msg = liftF (Send msg ())

recv :: (Monad m) => ProcessT msg m msg
recv = liftF (Recv id)

-- * Schedulers

roundRobin :: (Monad m) => [ProcessT msg m a] -> m ()
roundRobin = roundRobin' []

roundRobin' :: (Monad m) => [msg] -> [ProcessT msg m a] -> m ()
roundRobin' ms [] = return ()
roundRobin' ms (p:ps) = runFreeT p >>= \case
    Free (Tau action next) -> do
        x <- action
        roundRobin' ms (ps ++ [next x])
    Free (Send msg next) ->
        roundRobin' (ms ++ [msg]) (ps ++ [next])
    Free (Recv next) -> case ms of
        [] -> roundRobin' ms (ps ++ [p])
        (m:ms) -> roundRobin' ms (ps ++ [next m])
    Pure x ->
        roundRobin' ms ps

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
