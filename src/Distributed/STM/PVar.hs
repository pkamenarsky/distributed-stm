{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

module Distributed.STM.PVar where

import           Control.Monad                      (void, replicateM_)
import           Control.Exception
import           Control.Concurrent                 (forkIO)

import           Data.Aeson
import           Data.Int

import           Database.PostgreSQL.Simple
import           Database.PostgreSQL.Simple.SqlQQ

import           Distributed.STM

data PVar a = PVar String

newPVar :: ToJSON a => String -> a -> Atom (PVar a)
newPVar label val = do
  c <- connection
  unsafeAtomIO $ do
    execute c
      [sql| INSERT INTO variable (label, value)
            VALUES (?, ?) |]
      (label, toJSON val) `catch` exists
  return $ PVar label
  where exists :: SqlError -> IO Int64
        exists _ = return 0

readPVar :: FromJSON a => PVar a -> Atom a
readPVar (PVar label) = do
  c <- connection
  x <- unsafeAtomIO $ query c
         [sql| SELECT value FROM variable
               WHERE label = ? |]
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
    {-
    execute c [sql| DELETE FROM variable WHERE label = ? |] (Only label)
    execute c
      [sql| INSERT INTO variable (label, value)
            VALUES (?, ?) |]
      (label, toJSON val)
    -}
    execute c
      [sql| UPDATE variable SET value = ?
            WHERE label = ? |]
      (toJSON val, label)

testSTM :: IO ()
testSTM = do
  conn <- connectPostgreSQL  "host=localhost port=5432 dbname=postgres connect_timeout=10"
  initSTM conn

  v <- atomically conn $ newPVar "var" (5 :: Int)

  r <- atomically conn $ do
    r <- readPVar v
    writePVar v (r + 1)
    return (r + 1)

  replicateM_ 5 $ forkIO $ do
    r <- atomically conn $ do
      r <- readPVar v
      writePVar v (r + 1)
      return (r + 1)
    putStrLn $ "forked: " ++ show r

  putStrLn $ show r
