{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

module Distributed.STM where

import           Control.Monad.RWS

import           Database.PostgreSQL.Simple
import           Database.PostgreSQL.Simple.SqlQQ
import           Database.PostgreSQL.Simple.Transaction

newtype Atom a = Atom (RWST Connection () () IO a)
  deriving (Functor, Applicative, Monad)

connection :: Atom Connection
connection = Atom ask

unsafeAtomIO :: IO a -> Atom a
unsafeAtomIO a = Atom $ liftIO a

runAtom :: Connection -> Atom a -> IO a
runAtom conn (Atom atom) =
  fst <$> evalRWST atom conn ()

atomically :: Connection -> Atom a -> IO a
atomically conn atom = withTransactionSerializable conn $ runAtom conn atom

initSTM :: Connection -> IO ()
initSTM conn = void $ execute conn
  [sql| CREATE TABLE IF NOT EXISTS variable
        -- ( label     VARCHAR(256) NOT NULL PRIMARY KEY INITIALLY DEFERRED
        ( label     VARCHAR(256) NOT NULL PRIMARY KEY
        , value     JSONB
        ) |] ()
