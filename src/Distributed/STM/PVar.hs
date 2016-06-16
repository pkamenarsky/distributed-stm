{-# LANGUAGE QuasiQuotes #-}

module Distributed.STM.PVar where

import           Control.Monad                      (void)

import           Data.Aeson

import           Database.PostgreSQL.Simple
import           Database.PostgreSQL.Simple.SqlQQ

import           Distributed.STM

data PVar a = PVar String

newPVar :: ToJSON a => String -> a -> Atom (PVar a)
newPVar label val = do
  c <- connection
  unsafeAtomIO $ execute c
     [sql| INSERT INTO variable (label, value)
           VALUES (?, ?) |]
     (label, encode val)
  return $ PVar label

readPVar :: FromJSON a => PVar a -> Atom a
readPVar (PVar label) = do
  c <- connection
  x <- unsafeAtomIO $ query c
     [sql| SELECT value FROM variable
           WHERE label = ? |]
     (Only label)
  case x of
    (Only x:_) -> case decode x of
      Just x -> return x
      Nothing -> error "PVar can't be parsed"
    _ -> error "PVar doesn't exist"


writePVar :: ToJSON a => PVar a -> a -> Atom ()
writePVar (PVar label) val = do
  c <- connection
  void $ unsafeAtomIO $ do
    execute c [sql| DELETE FROM variable WHERE label = ? |] (Only label)
    execute c
       [sql| INSERT INTO variable (label, value)
             VALUES (?, ?) |]
       (label, encode val)
