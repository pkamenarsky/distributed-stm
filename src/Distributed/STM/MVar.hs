{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | 
-- An @'MVar' t@ is a distributed mutable location,
-- backed by a Postgres database, that is either empty or contains a
-- value of type @t@.  It has two fundamental operations: 'putMVar'
-- which fills an 'MVar' if it is empty and blocks otherwise, and
-- 'takeMVar' which empties an 'MVar' if it is full and blocks
-- otherwise.  They can be used in multiple different ways:
--
--   1. As distributed synchronized mutable variables,
--
--   2. As distributed channels, with 'takeMVar' and 'putMVar' as receive and send, and
--
--   3. As a distributed binary semaphore @'MVar' ()@, with 'takeMVar' and 'putMVar' as
--      wait and signal.
module Distributed.STM.MVar
  ( MVar

  , newEmptyMVar

  , putMVar
  , takeMVar
  ) where

import           Control.Monad                      (void, replicateM_)
import           Control.Exception
import           Control.Concurrent                 (forkIO, killThread, threadDelay)
import           Control.Concurrent.Chan
import qualified Control.Concurrent as C
import           Control.Monad.Reader

import           Data.Aeson
import qualified Data.ByteString as B
import           Data.Int
import           Data.IORef
import           Data.Pool
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import           Database.PostgreSQL.Simple
import           Database.PostgreSQL.Simple.SqlQQ
import           Database.PostgreSQL.Simple.Notification
import           Database.PostgreSQL.Simple.Transaction

import           Distributed.STM

data MVar a = MVar T.Text

newEmptyMVar :: Pool Connection -> T.Text -> IO (MVar a)
newEmptyMVar pool label = withResource pool $ \conn -> do
  liftIO $ do
    execute conn
      [sql| INSERT INTO variable (label, value)
            VALUES (?, ?)
      |]
      (label, Null) `catch` exists
  return $ MVar label
  where
    exists :: SqlError -> IO Int64
    exists _ = return 0

--------------------------------------------------------------------------------

data MVarException = Retry deriving Show
instance Exception MVarException

takeMVar :: FromJSON a => Pool Connection -> MVar a -> IO a
takeMVar pool mvar@(MVar label) = withResource pool $ \conn -> do
  r <- liftIO $ flip catch stmerror $ withTransactionSerializable conn $ do
    r <- query conn
           [sql| UPDATE variable SET value = ?
                 WHERE label = ?
                 RETURNING (SELECT value FROM variable WHERE label = ?)
           |]
       (Null, label, label)

    execute conn [sql| NOTIFY read_var, ? |] (Only label)

    case r of
      (Only Null:_) -> throw Retry
      (Only json:_) -> case fromJSON json of
        Success a -> pure (Right a) 
        Error e   -> error e
  case r of
    Left () -> do
      liftIO $ wait conn
      takeMVar pool mvar
    Right a -> pure a
  where
    stmerror Retry = pure (Left ())

    wait conn = do
      execute_ conn "LISTEN write_var"
      n <- getNotification conn
      execute_ conn "UNLISTEN write_var"
      unless (notificationChannel n == "write_var" && notificationData n == T.encodeUtf8 label)
        (wait conn)

putMVar :: ToJSON a => Pool Connection -> MVar a -> a -> IO ()
putMVar pool mvar@(MVar label) a = withResource pool $ \conn -> do
  r <- liftIO $ flip catch stmerror $ withTransactionSerializable conn $ do
    r <- query conn
           [sql| UPDATE variable SET value = ?
                 WHERE label = ?
                 RETURNING (SELECT value FROM variable WHERE label = ?)
           |]
       (toJSON a, label, label)

    execute conn [sql| NOTIFY write_var, ? |] (Only label)

    case r of
      (Only Null:_) -> pure (Right ())
      (Only json:_) -> throw Retry
  case r of
    Left () -> do
      liftIO $ wait conn
      putMVar pool mvar a
    Right () -> pure ()
  where
    stmerror Retry = pure (Left ())

    wait conn = do
      execute_ conn "LISTEN read_var"
      n <- getNotification conn
      execute_ conn "UNLISTEN read_var"
      unless (notificationChannel n == "read_var" && notificationData n == T.encodeUtf8 label)
        (wait conn)

withMVar :: FromJSON a => ToJSON a => Pool Connection -> MVar a -> (a -> IO (a, b)) -> IO b
withMVar pool mvar@(MVar label) f = withResource pool $ \conn -> do
  r <- liftIO $ flip catch stmerror $ withTransactionSerializable conn $ do
    r <- query conn
           [sql| UPDATE variable SET value = ?
                 WHERE label = ?
                 RETURNING (SELECT value FROM variable WHERE label = ?)
           |]
       (Null, label, label)

    (a, b) <- case r of
      (Only Null:_) -> throw Retry
      (Only json:_) -> case fromJSON json of
        Success a -> f a
        Error e   -> error e

    execute conn
      [sql| UPDATE variable SET value = ?
            WHERE label = ?
      |]
      (toJSON a, label)

    execute conn [sql| NOTIFY write_var, ? |] (Only label)

    pure (Right b)

  case r of
    Left () -> do
      liftIO $ wait conn
      withMVar pool mvar f
    Right b -> pure b
  where
    stmerror Retry = pure (Left ())

    wait conn = do
      execute_ conn "LISTEN write_var"
      n <- getNotification conn
      execute_ conn "UNLISTEN write_var"
      unless (notificationChannel n == "write_var" && notificationData n == T.encodeUtf8 label)
        (wait conn)

--------------------------------------------------------------------------------

testVar :: IO (Pool Connection, MVar Int)
testVar = do
  p <- createPool (connectPostgreSQL  "host=localhost port=5432 dbname=transportapiv3dev_phil user=transportapi password=randompasswordsFTWbutthiswillhavetodo") close 1 1 50
  v <- newEmptyMVar p "lock"
  pure (p, v)
