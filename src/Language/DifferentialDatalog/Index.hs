{-
Copyright (c) 2019 VMware, Inc.
SPDX-License-Identifier: MIT

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
-}

{-# LANGUAGE TupleSections, LambdaCase, RecordWildCards, FlexibleContexts #-}

{- | 
Module     : Index
Description: Helper functions for manipulating Indexes.
-}
module Language.DifferentialDatalog.Index (
    idxIdentifier,
    idxRelation,
    idxKeyType,
    indexValidate
) 
where

import qualified Data.Map as M
import Data.List
import Control.Monad.Except

import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.Rule
import Language.DifferentialDatalog.Expr

-- | Unique id, assigned to the index
idxIdentifier :: DatalogProgram -> Index -> Int
idxIdentifier d idx = M.findIndex (name idx) $ progIndexes d

idxRelation :: DatalogProgram -> Index -> Relation
idxRelation d idx = getRelation d $ atomRelation $ idxAtom idx

idxKeyType :: Index -> Type
idxKeyType = tTuple . map typ . idxVars

indexValidate :: (MonadError String me) => DatalogProgram -> Index -> me ()
indexValidate d idx@Index{..} = do
    fieldsValidate d [] idxVars
    atomValidate d (CtxIndex idx) idxAtom
    check (exprIsPatternImpl $ atomVal idxAtom) (pos idxAtom)
          $ "Index expression is not a pattern"
    -- Atom is defined over exactly the variables in the index.
    -- (variables in 'atom_vars \\ idx_vars' should be caught by 'atomValidate'
    -- above, so we only need to check for 'idx_vars \\ atom_vars' here).
    let idx_vars = map name idxVars
    let atom_vars = exprFreeVars d (CtxIndex idx) (atomVal idxAtom)
    check (null $ idx_vars \\ atom_vars) (pos idx)
          $ "The following index variables are not constrained by the index pattern: " ++
            (show $ idx_vars \\ atom_vars)

