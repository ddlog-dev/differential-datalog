{-
Copyright (c) 2018-2019 VMware, Inc.
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

{-# LANGUAGE RecordWildCards, LambdaCase, FlexibleContexts #-}

module Language.DifferentialDatalog.Rule (
    rULE_ATTR_MULTIWAY,
    rulePPPrefix,
    ruleRHSVars,
    ruleRHSNewVars,
    ruleVars,
    ruleRHSTermVars,
    ruleLHSVars,
    ruleTypeMapM,
    ruleHasJoins,
    ruleAggregateTypeParams,
    atomVarOccurrences,
    atomVars,
    ruleIsDistinctByConstruction,
    ruleIsRecursive,
    ruleIsMultiway,
    ruleValidate,
    atomValidate,
    ruleMoveRHSLeft,
    ruleMoveCondsUp,
    ruleOptimizeJoins
) where

import qualified Data.Set as S
import qualified Data.Map as M
import Data.List
import Data.Maybe
import Data.Either
--import Debug.Trace
import Text.PrettyPrint
import Control.Monad.Except

import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.PP
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Syntax
import {-# SOURCE #-} Language.DifferentialDatalog.Type
import {-# SOURCE #-} Language.DifferentialDatalog.Expr
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Relation

rULE_ATTR_MULTIWAY :: String
rULE_ATTR_MULTIWAY = "multiway"

-- | Pretty-print the first 'len' literals of a rule. 
rulePPPrefix :: Rule -> Int -> Doc
rulePPPrefix rl len = commaSep $ map pp $ take len $ ruleRHS rl

-- | New variables declared in the 'i'th conjunct in the right-hand
-- side of a rule.
ruleRHSNewVars :: DatalogProgram -> Rule -> Int -> [Field]
ruleRHSNewVars d rule idx =
    S.toList $ ruleRHSVarSet' d rule idx S.\\ ruleRHSVarSet' d rule (idx-1)

ruleRHSVars :: DatalogProgram -> Rule -> Int -> [Field]
ruleRHSVars d rl i = S.toList $ ruleRHSVarSet d rl i

-- | Variables visible in the 'i'th conjunct in the right-hand side of
-- a rule.  Does not include variables introduced in this conjunct.
-- All conjuncts before 'i' must be validated before calling this
-- function.
ruleRHSVarSet :: DatalogProgram -> Rule -> Int -> S.Set Field
ruleRHSVarSet d rl i = ruleRHSVarSet' d rl (i-1)

-- Variables visible _after_ 'i'th conjunct.
ruleRHSVarSet' :: DatalogProgram -> Rule -> Int -> S.Set Field
ruleRHSVarSet' _ _  i | i < 0 = S.empty
ruleRHSVarSet' d rl i =
    case ruleRHS rl !! i of
         RHSLiteral _ True  _ _           -> vs `S.union` (atomVarDecls d rl i)
         RHSLiteral _ False _ _           -> vs
         -- assignment introduces new variables
         RHSCondition _ (E e@(ESet _ l _)) -> vs `S.union` exprDecls d (CtxSetL e (CtxRuleRCond rl i)) l
         -- condition does not introduce new variables
         RHSCondition _ _                -> vs
         -- FlatMap introduces a variable
         RHSFlatMap _ v e                -> let t = case exprType' d (CtxRuleRFlatMap rl i) e of
                                                         TOpaque _ _         [t']    -> t'
                                                         TOpaque _ tname     [kt,vt] | tname == mAP_TYPE
                                                                                     -> tTuple [kt,vt]
                                                         t' -> error $ "Rule.ruleRHSVarSet': unexpected FlatMap type " ++ show t'
                                            in S.insert (Field nopos v t) vs
         -- Aggregation hides all variables except groupBy vars
         -- and the aggregate variable
         RHSAggregate _ avar gvars fname _ -> let ctx = CtxRuleRAggregate rl i
                                                  gvars' = map (getVar d ctx) gvars
                                                  f = getFunc d fname
                                                  tmap = ruleAggregateTypeParams d rl i
                                                  atype = typeSubstTypeArgs tmap $ funcType f
                                                  avar' = Field nopos avar atype
                                              in S.fromList $ avar':gvars'
    where
    vs = ruleRHSVarSet d rl i

ruleAggregateTypeParams :: DatalogProgram -> Rule -> Int -> M.Map String Type
ruleAggregateTypeParams d rl idx =
    case ruleCheckAggregate d rl idx of
         Left e -> error $ "ruleAggregateTypeParams: " ++ e
         Right tmap -> tmap

exprDecls :: DatalogProgram -> ECtx -> Expr -> S.Set Field
exprDecls d ctx e =
    S.fromList
        $ map (\(v, ctx') -> Field nopos v $ exprType d ctx' (eVarDecl v))
        $ exprVarDecls ctx e

atomVarTypes :: DatalogProgram -> ECtx -> Expr -> [Field]
atomVarTypes d ctx e =
    map (\(v, ctx') -> Field nopos v $ exprType d ctx' (eVar v))
        $ atomVarOccurrences ctx e

atomVarOccurrences :: ECtx -> Expr -> [(String, ECtx)]
atomVarOccurrences ctx e =
    exprCollectCtx (\ctx' e' ->
                    case e' of
                         EVar _ v       -> [(v, ctx')]
                         EBinding _ v _ -> [(v, ctx')]
                         _              -> [])
                   (++) ctx e

atomVars :: Expr -> [String]
atomVars e =
    exprCollect (\case
                  EVar _ v       -> [v]
                  EBinding _ v _ -> [v]
                  _              -> [])
                (++) e

atomVarDecls :: DatalogProgram -> Rule -> Int -> S.Set Field
atomVarDecls d rl i = S.fromList $ atomVarTypes d (CtxRuleRAtom rl i) (atomVal $ rhsAtom $ ruleRHS rl !! i)

-- | Variables used in a RHS term of a rule
ruleRHSTermVars :: DatalogProgram -> Rule -> Int -> [String]
ruleRHSTermVars d rl i =
    case ruleRHS rl !! i of
         RHSLiteral{..}   -> exprFreeVars d (CtxRuleRAtom rl i) $ atomVal rhsAtom
         RHSCondition{..} -> exprFreeVars d (CtxRuleRCond rl i) rhsExpr
         RHSFlatMap{..}   -> exprFreeVars d (CtxRuleRFlatMap rl i) rhsMapExpr
         RHSAggregate{..} -> nub $ rhsGroupBy ++ exprFreeVars d (CtxRuleRAggregate rl i) rhsAggExpr

-- | All variables visible after the last RHS clause of the rule
ruleVars :: DatalogProgram -> Rule -> [Field]
ruleVars d rl@Rule{..} = ruleRHSVars d rl (length ruleRHS)

-- | Variables used in the head of the rule
ruleLHSVars :: DatalogProgram -> Rule -> [Field]
ruleLHSVars d rl = S.toList $ ruleLHSVarSet d rl

ruleLHSVarSet :: DatalogProgram -> Rule -> S.Set Field
ruleLHSVarSet d rl = S.fromList
    $ concat
    $ mapIdx (\a i -> atomVarTypes d (CtxRuleL rl i) $ atomVal a)
    $ ruleLHS rl

-- | Map function over all types in a rule
ruleTypeMapM :: (Monad m) => (Type -> m Type) -> Rule -> m Rule
ruleTypeMapM fun rule@Rule{..} = do
    lhs <- mapM (\(Atom p r v) -> Atom p r <$> exprTypeMapM fun v) ruleLHS
    rhs <- mapM (\rhs -> case rhs of
                  RHSLiteral p pol (Atom pa r v) d -> (\v' -> RHSLiteral p pol (Atom pa r v') d) <$> exprTypeMapM fun v
                  RHSCondition p c                 -> RHSCondition p <$> exprTypeMapM fun c
                  RHSAggregate p v g f e           -> RHSAggregate p v g f <$> exprTypeMapM fun e
                  RHSFlatMap p v e                 -> RHSFlatMap p v <$> exprTypeMapM fun e)
                ruleRHS
    return rule { ruleLHS = lhs, ruleRHS = rhs }

-- | 'True' iff the rule contains at least one join or antijoin operation.
ruleHasJoins :: Rule -> Bool
ruleHasJoins rule = ruleNumJoins rule > 0

-- | Count the number of join and antijoin operations in a rule.
ruleNumJoins :: Rule -> Int
ruleNumJoins rule =
    length $ filter rhsIsLiteral $ tail $ ruleRHS rule

-- | Checks if a rule (more precisely, the given head of the rule) yields a
-- relation with distinct elements.
ruleIsDistinctByConstruction :: DatalogProgram -> Rule -> Int -> Bool
ruleIsDistinctByConstruction d rl@Rule{..} head_idx = f True 0
    where
    head_atom = ruleLHS !! head_idx
    headrel = atomRelation head_atom
    -- Relation is distinct wrt 'headrel'.
    relIsDistinct' :: String -> Bool
    relIsDistinct' rel =
        -- ..and we _are_ using 'rel' from the top-level scope.
        not (relsAreMutuallyRecursive d rel headrel) &&
        -- 'rel' is distinct in the top-level scope..
        relIsDistinct d (getRelation d rel)

    -- Recurse over the body of the rule, checking if the output of each
    -- prefix is a distinct relation.
    --
    -- If the first argument is 'Just vs', then the prefix of the body generates
    -- a distincts relation over 'vs'; if it is 'Nothing' then the prefix of the
    -- rule outputs a non-distinct relation.
    f :: Bool -> Int -> Bool
    f False i | i == length ruleRHS         = False
    f True  i | i == length ruleRHS         =
        -- The head of the rule is an injective function of all the variables in its body
        exprIsInjective d (CtxRuleL rl head_idx) (S.fromList $ map name $ ruleVars d rl) (atomVal head_atom)
    f True i | rhsIsCondition (ruleRHS !! i)
                                            = f True (i + 1)
    -- Aggregate operator returns a distinct collection over group-by and aggregate
    -- variables even if the prefix before isn't distinct.
    f _    i | rhsIsAggregate (ruleRHS !! i)= f True (i + 1)
    f True i | rhsIsPositiveLiteral (ruleRHS !! i)
                                            =
        let a = rhsAtom $ ruleRHS !! i in
        -- 'a' is a distinct relation and does not contain wildcards
        if relIsDistinct' (atomRelation a) && (not $ exprContainsPHolders $ atomVal a)
           then f True (i+1)
           else f False (i+1)
    -- Antijoins preserve distinctness
    f True i | rhsIsNegativeLiteral (ruleRHS !! i)
                                            = f True (i + 1)
    f _ i                                   = f False (i + 1)

-- | Checks if a rule (more precisely, the given head of the rule) is part of a
-- recursive fragment of the program.
ruleIsRecursive :: DatalogProgram -> Rule -> Int -> Bool
ruleIsRecursive d Rule{..} head_idx =
    let head_atom = ruleLHS !! head_idx in
    any (relsAreMutuallyRecursive d (atomRelation head_atom))
        $ map (atomRelation . rhsAtom)
        $ filter rhsIsLiteral ruleRHS

-- | 'True' iff the rule must be evaluated as a multiway join.
ruleIsMultiway :: Rule -> Bool
ruleIsMultiway = any ((== rULE_ATTR_MULTIWAY) . name) . ruleAttrs

ruleValidate :: (MonadError String me) => DatalogProgram -> Rule -> me ()
ruleValidate d rl@Rule{..} = do
    when (not $ null ruleRHS) $ do
        case head ruleRHS of
             RHSLiteral _ True _ _ -> return ()
             _                     -> err (pos rl) "Rule must start with positive literal"
    mapIdxM_ (ruleRHSValidate d rl) ruleRHS
    mapIdxM_ (ruleLHSValidate d rl) ruleLHS
    uniqNames ("Multiple definitions of attribute " ++) ruleAttrs
    mapM_ (ruleValidateAttr d rl) ruleAttrs

atomValidate :: (MonadError String me) => DatalogProgram -> ECtx -> Atom -> me ()
atomValidate d ctx atom = do
    _ <- checkRelation (pos atom) d $ atomRelation atom
    exprValidate d [] ctx $ atomVal atom
    let vars = ctxAllVars d ctx
    -- variable cannot be declared and used in the same atom
    uniq' (\_ -> pos atom) fst (\(v,_) -> "Variable " ++ v ++ " is both declared and used inside relational atom " ++ show atom)
        $ filter (\(var, _) -> isNothing $ find ((==var) . name) vars)
        $ atomVarOccurrences ctx $ atomVal atom

ruleRHSValidate :: (MonadError String me) => DatalogProgram -> Rule -> RuleRHS -> Int -> me ()
ruleRHSValidate d rl@Rule{..} RHSLiteral{..} idx =
    atomValidate d (CtxRuleRAtom rl idx) rhsAtom

ruleRHSValidate d rl@Rule{..} RHSCondition{..} idx =
    exprValidate d [] (CtxRuleRCond rl idx) rhsExpr

ruleRHSValidate d rl@Rule{..} RHSFlatMap{..} idx = do
    let ctx = CtxRuleRFlatMap rl idx
    exprValidate d [] ctx rhsMapExpr
    checkIterable "FlatMap expression" (pos rhsMapExpr) d $ exprType d ctx rhsMapExpr

ruleRHSValidate d rl RHSAggregate{} idx = do
    _ <- ruleCheckAggregate d rl idx
    return ()

ruleLHSValidate :: (MonadError String me) => DatalogProgram -> Rule -> Atom -> Int -> me ()
ruleLHSValidate d rl a@Atom{..} idx = do
    rel <- checkRelation atomPos d atomRelation
    when (relRole rel == RelInput) $ check (null $ ruleRHS rl) (pos a)
         $ "Input relation " ++ name rel ++ " cannot appear in the head of a rule"
    exprValidate d [] (CtxRuleL rl idx) atomVal

ruleValidateAttr :: (MonadError String me) => DatalogProgram -> Rule -> Attribute -> me ()
ruleValidateAttr d rl attr = do
    if name attr == rULE_ATTR_MULTIWAY
       then case attrVal attr of
                 E (EBool _ True) -> checkRuleAllowsMultiwayJoin d rl
                 _ -> err (pos attr) $ "#[" ++ rULE_ATTR_MULTIWAY ++ "] attribute does not take a value"
       else err (pos attr) $ "Unknown attribute " ++ name attr

-- Validate Aggregate term, compute type argument map for the aggregate function used in the term.
-- e.g., given an aggregate function:
-- extern function group2map(g: Group<('K,'V)>): Map<'K,'V>
--
-- and its invocation:
-- Aggregate4(x, map) :- AggregateMe1(x,y), Aggregate((x), map = group2map((x,y)))
--
-- compute concrete types for 'K and 'V
ruleCheckAggregate :: (MonadError String me) => DatalogProgram -> Rule -> Int -> me (M.Map String Type)
ruleCheckAggregate d rl idx = do
    let RHSAggregate _ v vs fname e = ruleRHS rl !! idx
    let ctx = CtxRuleRAggregate rl idx
    exprValidate d [] ctx e
    -- group-by variables are visible in this scope
    mapM_ (checkVar (pos e) d ctx) vs
    check (notElem v vs) (pos e) $ "Aggregate variable " ++ v ++ " already declared in this scope"
    -- aggregation function exists and takes a group as its sole argument
    f <- checkFunc (pos e) d fname
    check (length (funcArgs f) == 1) (pos e) $ "Aggregation function must take one argument, but " ++
                                               fname ++ " takes " ++ (show $ length $ funcArgs f) ++ " arguments"
    -- figure out type of the aggregate
    funcTypeArgSubsts d (pos e) f [tOpaque gROUP_TYPE [exprType d ctx e]]

checkRuleAllowsMultiwayJoin :: (MonadError String me) => DatalogProgram -> Rule -> me ()
checkRuleAllowsMultiwayJoin d rl = do
    check (all (\rhs -> rhsIsPositiveLiteral rhs || rhsIsFilterCondition rhs) $ ruleRHS rl) (pos rl) $
          "Rule cannot be evaluated using multiway join: " ++
          "only rules without antijoins, aggregates, variable assignments, and flatmaps can be declared as #[" ++ rULE_ATTR_MULTIWAY ++ "]"
    mapM_ (\rhs -> when (rhsIsPositiveLiteral rhs)
                        $ check (exprIsPatternImpl $ atomVal $ rhsAtom rhs) (pos rhs)
                        $ "Literal contains sub-expression that cannot be evaluated using multiway join: " ++
                          "#[" ++ rULE_ATTR_MULTIWAY ++ "] requires that all literals in the rule use pattern syntax")
          $ ruleRHS rl
    check (ruleNumJoins rl > 1) (pos rl) $
          "Rule cannot be evaluated using multiway join: " ++
          "only rules with two or more joins can be declared as #[" ++ rULE_ATTR_MULTIWAY ++ "]"
    check (not $ ruleIsRecursive d rl 0) (pos rl) $
          "#[" ++ rULE_ATTR_MULTIWAY ++ "] is not supported for recursive rules"

-- | Move the term at position 'fromidx' in the RHS of the rule
-- to position 'toidx <= fromidx'.
ruleMoveRHSLeft :: Rule -> Int -> Int -> Rule
ruleMoveRHSLeft rl@Rule{..} fromidx toidx =
    rl { ruleRHS = rhs'' }
    where
    rhs' = take fromidx ruleRHS ++ drop (fromidx+1) ruleRHS
    rhs'' = take toidx rhs' ++ [ruleRHS !! fromidx] ++ drop toidx rhs'

-- | Move a condition term to the earliest position in the body of a rule
-- where all variables it depends on are defined.
--
-- Transforms, e.g.,
-- 'H(x,z) :- A(x,y), B(y,z), cond(x).'
-- into
-- 'H(x,z) :- A(x,y), cond(x), B(y,z).'
--
-- 'rl' - input rule.
-- 'i'  - index of the condition in the body of the rule;
-- must point to 'RHSCondition'.
--
-- Returns transformed rule.
ruleMoveCondUp :: DatalogProgram -> Rule -> Int -> Rule

-- Cannot move beyond first literal.
ruleMoveCondUp _ rl 1 = rl

-- Do not move past aggregation
ruleMoveCondUp _ rl i | rhsIsAggregate (ruleRHS rl !! (i-1)) = rl

ruleMoveCondUp d rl i | (S.fromList $ ruleRHSTermVars d rl i) `S.isSubsetOf` (S.map name $ ruleRHSVarSet d rl (i-1))
                      = ruleMoveCondUp d (ruleMoveRHSLeft rl i (i-1)) (i-1)
                      | otherwise
                      = rl

-- | Optimize a rule by moving all conditions in the body of the rule to the
-- earliest possible position.
ruleMoveCondsUp :: DatalogProgram -> Rule -> Rule
ruleMoveCondsUp d rl = foldl' (\rl' i -> ruleMoveCondUp d rl' i) rl filters
    where filters = findIndices rhsIsFilterCondition $ ruleRHS rl

-- | Rewrites the body of the rule by picking the next literal to join the
-- prefix of length 'prefix' with and moving this literal to RHS position
-- 'prefix'.
--
-- Picks a literal that shares the largest number of variables with the prefix.
--
-- Transforms, e.g.,
-- 'H(x,z) :- A(x,y), B(z, w), C(y,z).'
-- into
-- 'H(x,z) :- A(x,y), C(y, z), B(z,w).'
--
-- This function searches for a candidate literal up to the next Aggregate,
-- antijoin, or FlatMap operator.  It does not move RHSConditions, which may
-- have to be optimized using ruleMoveCondsUp.
--
-- 'rl' - input rule.
-- 'i'  - size of already arranged RHS prefix.
--
-- Returns transformed rule.
--
-- TODO: smarter optimizations are possible, e.g., we could break ties by
-- picking literals that maximize arrangement reuse.
rulePickNextJoin :: DatalogProgram -> Rule -> Int -> Rule
rulePickNextJoin d rl@Rule{..} prefix =
    -- Swap 'i' and 'j'
    case ranked of
         []     -> rl
         best:_ -> ruleMoveRHSLeft rl best prefix
    where
    -- Variables visible after the prefix.
    vars_after_prefix = map name $ ruleRHSVars d rl prefix
    -- Joins between 'prefix' and the end of the rule or the next non-filter
    -- and non-join operator.
    candidates = -- Check if reordering is possible (e.g., we wouldn't be able to re-order:
                 -- `A(x,y), B(x+1, z)`).
                 filter (\i -> isRight $ ruleValidate d $ ruleMoveRHSLeft rl i prefix)
                 $ map (prefix+)
                 $ findIndices rhsIsLiteral
                 $ takeWhile (\rhs -> rhsIsLiteral rhs || rhsIsFilterCondition rhs)
                 $ drop prefix ruleRHS
    -- Compute quality score for each candidate.
    scores = map (\i -> length $ ruleRHSTermVars d rl i `intersect` vars_after_prefix)
             candidates
    -- Sort by score (descending)
    ranked = map fst
             $ sortBy (\(_,s1) (_,s2) -> s2 `compare` s1)
             $ zip candidates scores

-- | Optimizes the rule by iteratively applying `rulePickNextJoin` to rearrange all
-- joins following the prefix of length 'i'.
ruleOptimizeJoins :: DatalogProgram -> Rule -> Int -> Rule
ruleOptimizeJoins d rl prefix | prefix == length (ruleRHS rl)  = rl
                              | otherwise =
    ruleOptimizeJoins d (rulePickNextJoin d rl prefix) (prefix+1)
