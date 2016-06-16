{-# LANGUAGE QuasiQuotes #-}

module Distributed.STM.PVar where

import           Database.PostgreSQL.Simple
import           Database.PostgreSQL.Simple.SqlQQ

import           Distributed.STM

data PVar a = PVar String

newPVar :: String -> a -> Atom (PVar a)
newPVar label val = do
  c <- connection
  unsafeAtomIO $ execute c
     [sql| INSERT INTO variable (label, value)
           VALUES (?, ?) |]
     (label, Binary $ encode val)
  return $ PVar label
