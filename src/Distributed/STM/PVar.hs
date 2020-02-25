{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

module Distributed.STM.PVar where

import           Control.Monad                      (void, replicateM_)
import           Control.Exception
import           Control.Concurrent                 (forkIO, killThread, threadDelay)
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

newtype STM a = STM (ReaderT (Connection, IORef (M.Map B.ByteString [C.MVar B.ByteString])) IO a)

runSTM :: Connection -> STM a -> IO a
runSTM conn (STM stm) = do
  mvar <- newIORef M.empty

  -- TODO: exceptions
  tid <- forkIO $ do
    n  <- getNotification conn

    io <- atomicModifyIORef' mvar $ \m -> case M.lookup (notificationChannel n) m of
      -- fire the next listener
      Just (mvar:mvars) ->
        ( M.insert (notificationChannel n) mvars m
        , C.putMVar mvar (notificationData n)
        )
      -- no more listeners, UNLISTEN
      Just []       ->
        ( M.delete (notificationChannel n) m
        , void $ execute conn [sql| UNLISTEN ? |] (Only (notificationChannel n))
        )
    io

  a <- runReaderT stm (conn, mvar)
  killThread tid
  pure a

newEmptyMVar :: T.Text -> STM (MVar a)
newEmptyMVar label = STM $ do
  (conn, _) <- ask
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

readMVar :: FromJSON a => MVar a -> STM a
readMVar (MVar label) = STM $ ask >>= loop
  where
    register mvar (Just mvars) = Just (mvar:mvars)
    register mvar Nothing = Just [mvar]

    loop (conn, mvar) = do
      a <- liftIO $ read conn
      case a of
        Left _  -> do
          liftIO $ do
            listener <- C.newEmptyMVar
            modifyIORef mvar $ \m -> M.alter (register listener) (T.encodeUtf8 label) m
            execute conn [sql| LISTEN ? |] (Only label)
            C.takeMVar listener
          loop (conn, mvar)
        Right a -> pure a

    -- TODO: exceptions
    read conn = do
        flip beginMode conn $ TransactionMode
          { isolationLevel = Serializable
          , readWriteMode  = ReadWrite
          }
        r <- query conn
               [sql| UPDATE variable SET value = ?
                     WHERE label = ?
                     RETURNING (SELECT value FROM variable WHERE label = ?)
               |]
             (Null, label, label)
        case r of
          (Only Null:_) -> do
            rollback conn
            pure $ Left ()
          (Only json:_) -> case fromJSON json of
            Success a -> do
              commit conn
              pure $ Right a
            Error e   -> do
              rollback conn
              error e

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
