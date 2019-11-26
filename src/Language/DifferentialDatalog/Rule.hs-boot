{-# LANGUAGE FlexibleContexts #-}

module Language.DifferentialDatalog.Rule where

import Language.DifferentialDatalog.Syntax
import Control.Monad.Except

rULE_ATTR_MULTIWAY :: String
ruleRHSVars :: DatalogProgram -> Rule -> Int -> [Field]
ruleVars :: DatalogProgram -> Rule -> [Field]
ruleRHSTermVars :: DatalogProgram -> Rule -> Int -> [String]
ruleLHSVars :: DatalogProgram -> Rule -> [Field]
ruleTypeMapM :: (Monad m) => (Type -> m Type) -> Rule -> m Rule
ruleHasJoins :: Rule -> Bool
atomVarOccurrences :: ECtx -> Expr -> [(String, ECtx)]
atomVars :: Expr -> [String]
ruleIsDistinctByConstruction :: DatalogProgram -> Rule -> Int -> Bool
ruleIsRecursive :: DatalogProgram -> Rule -> Int -> Bool
ruleIsMultiway :: Rule -> Bool
ruleValidate :: (MonadError String me) => DatalogProgram -> Rule -> me ()
