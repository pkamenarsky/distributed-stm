{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

module Distributed.STM.PVar
  ( MVar

  , newEmptyMVar

  , putMVar
  , readMVar
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

newtype STM a = STM { unSTM :: ReaderT Connection IO a }

runSTM :: Pool Connection -> STM a -> IO a
runSTM pool (STM stm) = withResource pool (runReaderT stm)

newEmptyMVar :: T.Text -> STM (MVar a)
newEmptyMVar label = STM $ do
  conn <- ask
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

data MVarException = Retry deriving Show
instance Exception MVarException

readMVar :: FromJSON a => MVar a -> STM a
readMVar mvar@(MVar label) = STM $ do
  conn <- ask
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
      unSTM (readMVar mvar)
    Right a -> pure a
  where
    stmerror Retry = pure (Left ())

    wait conn = do
      execute_ conn "LISTEN write_var"
      n <- getNotification conn
      execute_ conn "UNLISTEN write_var"
      unless (notificationChannel n == "write_var" && notificationData n == T.encodeUtf8 label)
        (wait conn)

putMVar :: ToJSON a => MVar a -> a -> STM ()
putMVar mvar@(MVar label) a = STM $ do
  conn <- ask
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
      unSTM (putMVar mvar a)
    Right () -> pure ()
  where
    stmerror Retry = pure (Left ())

    wait conn = do
      execute_ conn "LISTEN read_var"
      n <- getNotification conn
      execute_ conn "UNLISTEN read_var"
      unless (notificationChannel n == "read_var" && notificationData n == T.encodeUtf8 label)
        (wait conn)

withMVar :: FromJSON a => ToJSON a => MVar a -> (a -> IO (a, b)) -> STM b
withMVar mvar@(MVar label) f = STM $ do
  conn <- ask
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
      unSTM (withMVar mvar f)
    Right b -> pure b
  where
    stmerror Retry = pure (Left ())

    wait conn = do
      execute_ conn "LISTEN write_var"
      n <- getNotification conn
      execute_ conn "UNLISTEN write_var"
      unless (notificationChannel n == "write_var" && notificationData n == T.encodeUtf8 label)
        (wait conn)

getVar :: IO (Pool Connection, MVar Int)
getVar = do
  p <- createPool (connectPostgreSQL  "host=localhost port=5432 dbname=transportapiv3dev_phil user=transportapi password=randompasswordsFTWbutthiswillhavetodo") close 1 1 50
  v <- runSTM p (newEmptyMVar "lock")
  pure (p, v)

getVar2 :: IO (Pool Connection, MVar Int)
getVar2 = do
  p <- createPool (connectPostgreSQL  "host=localhost port=5432 dbname=transportapiv3dev_phil user=transportapi password=randompasswordsFTWbutthiswillhavetodo") close 1 1 50
  v <- runSTM p (newEmptyMVar "lock2")
  pure (p, v)

--------------------------------------------------------------------------------

data PVar a = PVar String

newPVar :: ToJSON a => String -> a -> Atom (PVar a)
newPVar label val = do
  c <- connection
  unsafeAtomIO $ do
    execute c
      [sql| INSERT INTO variable (label, value)
            VALUES (?, ?)
      |]
      (label, toJSON val) `catch` exists
  return $ PVar label
  where
    exists :: SqlError -> IO Int64
    exists _ = return 0

readPVar :: FromJSON a => PVar a -> Atom a
readPVar (PVar label) = do
  c <- connection
  x <- unsafeAtomIO $ query c
         [sql| SELECT value FROM variable
               WHERE label = ?
         |]
         (Only label)
  case x of
    (Only x:_) -> case fromJSON x of
      Success x -> return x
      Error e -> error $ "PVar can't be parsed: " ++ e
    _ -> error "PVar doesn't exist"

writePVar :: ToJSON a => PVar a -> a -> Atom ()
writePVar (PVar label) val = do
  c <- connection
  void $ unsafeAtomIO $ do
    execute c
      [sql| UPDATE variable SET value = ?
            WHERE label = ?
      |]
      (toJSON val, label)

testSTM :: IO ()
testSTM = do
  pool <- createPool (connectPostgreSQL  "host=localhost port=5432 dbname=transportapiv3dev_phil user=transportapi password=randompasswordsFTWbutthiswillhavetodo") close 1 1 50
  withResource pool $ \conn -> execute_ conn [sql| DROP TABLE variable |]
        
  withResource pool $ \conn -> initSTM conn

  v <- withResource pool $ \conn -> atomically conn $ newPVar "var" (5 :: Int)

  r <- withResource pool $ \conn -> atomically conn $ do
    r <- readPVar v
    writePVar v (r + 1)
    return (r + 1)

  replicateM_ 5 $ forkIO $ do
    r <- withResource pool $ \conn -> atomically conn $ do
      r <- readPVar v
      writePVar v (r + 1)
      return (r + 1)
    putStrLn $ "forked: " ++ show r

  replicateM_ 5 $ forkIO $ do
    r <- withResource pool $ \conn -> atomically conn $ do
      r <- readPVar v
      writePVar v (r + 1)
      return (r + 1)
    putStrLn $ "forked: " ++ show r

  putStrLn $ show r
