{-
Copyright (c) 2018 VMware, Inc.
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

{-# LANGUAGE FlexibleContexts, ImplicitParams, RecordWildCards, LambdaCase, TupleSections #-}

module Language.DifferentialDatalog.Expr (
    exprMapM,
    exprMap,
    exprFold,
    exprFoldM,
    exprTraverseCtxWithM,
    exprTraverseCtxM,
    exprTraverseM,
    exprFoldCtx,
    exprFoldCtxM,
    exprCollectCtxM,
    exprCollectM,
    exprCollectCtx,
    exprCollect,
    exprVarOccurrences,
    exprVars,
    exprFreeVars,
    exprIsConst,
    exprVarDecls,
    exprFuncs,
    exprFuncsRec,
    isLVar,
    exprIsPattern,
    exprIsPatternImpl,
    exprContainsPHolders,
    exprIsDeconstruct,
    exprIsVarOrFieldLVal,
    exprIsVarOrField,
    exprIsInjective,
    exprTypeMapM,
    exprValidate
    ) where

import Data.List
import Data.Maybe
import Control.Monad.Identity
import Control.Monad.Except
import qualified Data.Set as S
--import Debug.Trace

import Language.DifferentialDatalog.Ops
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.ECtx

-- depth-first fold of an expression
exprFoldCtxM :: (Monad m) => (ECtx -> ExprNode b -> m b) -> ECtx -> Expr -> m b
exprFoldCtxM f ctx (E n) = exprFoldCtxM' f ctx n

exprFoldCtxM' :: (Monad m) => (ECtx -> ExprNode b -> m b) -> ECtx -> ENode -> m b
exprFoldCtxM' f ctx   (EVar p v)              = f ctx $ EVar p v
exprFoldCtxM' f ctx e@(EApply p fun as)       = f ctx =<< EApply p fun <$> (mapIdxM (\a i -> exprFoldCtxM f (CtxApply e ctx i) a) as)
exprFoldCtxM' f ctx e@(EField p s fl)         = do s' <- exprFoldCtxM f (CtxField e ctx) s
                                                   f ctx $ EField p s' fl
exprFoldCtxM' f ctx e@(ETupField p s fl)      = do s' <- exprFoldCtxM f (CtxTupField e ctx) s
                                                   f ctx $ ETupField p s' fl
exprFoldCtxM' f ctx   (EBool p b)             = f ctx $ EBool p b
exprFoldCtxM' f ctx   (EInt p i)              = f ctx $ EInt p i
exprFoldCtxM' f ctx   (EString p s)           = f ctx $ EString p s
exprFoldCtxM' f ctx   (EBit p w v)            = f ctx $ EBit p w v
exprFoldCtxM' f ctx   (ESigned p w v)         = f ctx $ ESigned p w v
exprFoldCtxM' f ctx e@(EStruct p c fs)        = f ctx =<< EStruct p c <$> (mapM (\(fname, fl) -> (fname,) <$> exprFoldCtxM f (CtxStruct e ctx fname) fl) fs)
exprFoldCtxM' f ctx e@(ETuple p fs)           = f ctx =<< ETuple p <$> (mapIdxM (\fl i -> exprFoldCtxM f (CtxTuple e ctx i) fl) fs)
exprFoldCtxM' f ctx e@(ESlice p v h l)        = do v' <- exprFoldCtxM f (CtxSlice e ctx) v
                                                   f ctx $ ESlice p v' h l
exprFoldCtxM' f ctx e@(EMatch p m cs)         = do m' <- exprFoldCtxM f (CtxMatchExpr e ctx) m
                                                   cs' <- mapIdxM (\(e1, e2) i -> (,) <$> (exprFoldCtxM f (CtxMatchPat e ctx i) e1) <*>
                                                                                          (exprFoldCtxM f (CtxMatchVal e ctx i) e2)) cs
                                                   f ctx $ EMatch p m' cs'
exprFoldCtxM' f ctx   (EVarDecl p v)          = f ctx $ EVarDecl p v
exprFoldCtxM' f ctx e@(ESeq p l r)            = f ctx =<< ESeq p <$> exprFoldCtxM f (CtxSeq1 e ctx) l <*>
                                                                     exprFoldCtxM f (CtxSeq2 e ctx) r
exprFoldCtxM' f ctx e@(EITE p i t el)         = f ctx =<< EITE p <$>
                                                          exprFoldCtxM f (CtxITEIf e ctx) i <*>
                                                          exprFoldCtxM f (CtxITEThen e ctx) t <*>
                                                          exprFoldCtxM f (CtxITEElse e ctx) el
exprFoldCtxM' f ctx e@(EFor p v i b)          = f ctx =<< EFor p v <$>
                                                          exprFoldCtxM f (CtxForIter e ctx) i <*>
                                                          exprFoldCtxM f (CtxForBody e ctx) b
exprFoldCtxM' f ctx e@(ESet p l r)            = do -- XXX: start with RHS, e.g., in validating an assignment it helps to know RHS type
                                                   -- before validating LHS
                                                   r' <- exprFoldCtxM f (CtxSetR e ctx) r
                                                   l' <- exprFoldCtxM f (CtxSetL e ctx) l
                                                   f ctx $ ESet p l' r'
exprFoldCtxM' f ctx e@(EBinOp p op l r)       = f ctx =<< EBinOp p op <$> exprFoldCtxM f (CtxBinOpL e ctx) l <*>
                                                                          exprFoldCtxM f (CtxBinOpR e ctx) r
exprFoldCtxM' f ctx e@(EUnOp p op x)          = f ctx =<< EUnOp p op <$> (exprFoldCtxM f (CtxUnOp e ctx) x)
exprFoldCtxM' f ctx   (EPHolder p)            = f ctx $ EPHolder p
exprFoldCtxM' f ctx e@(EBinding p v x)        = do x' <- exprFoldCtxM f (CtxBinding e ctx) x
                                                   f ctx $ EBinding p v x'
exprFoldCtxM' f ctx e@(ETyped p x t)          = do x' <- exprFoldCtxM f (CtxTyped e ctx) x
                                                   f ctx $ ETyped p x' t
exprFoldCtxM' f ctx e@(EAs p x t)             = do x' <- exprFoldCtxM f (CtxAs e ctx) x
                                                   f ctx $ EAs p x' t
exprFoldCtxM' f ctx e@(ERef p x)              = do x' <- exprFoldCtxM f (CtxRef e ctx) x
                                                   f ctx $ ERef p x'

exprMapM :: (Monad m) => (a -> m b) -> ExprNode a -> m (ExprNode b)
exprMapM g e = case e of
                   EVar p v            -> return $ EVar p v
                   EApply p f as       -> EApply p f <$> mapM g as
                   EField p s f        -> (\s' -> EField p s' f) <$> g s
                   ETupField p s f     -> (\s' -> ETupField p s' f) <$> g s
                   EBool p b           -> return $ EBool p b
                   EInt p i            -> return $ EInt p i
                   EString p s         -> return $ EString p s
                   EBit p w v          -> return $ EBit p w v
                   ESigned p w v       -> return $ ESigned p w v
                   EStruct p s fs      -> EStruct p s <$> mapM (\(fname, e') -> (fname,) <$> g e') fs
                   ETuple p fs         -> ETuple p <$> mapM g fs
                   ESlice p v h l      -> (\v' -> ESlice p v' h l) <$> g v
                   EMatch p m cs       -> EMatch p <$> g m <*> mapM (\(e1, e2) -> (,) <$> g e1 <*> g e2) cs
                   EVarDecl p v        -> return $ EVarDecl p v
                   ESeq p l r          -> ESeq p <$> g l <*> g r
                   EITE p i t el       -> EITE p <$> g i <*> g t <*> g el
                   EFor p v i b        -> EFor p v <$> g i <*> g b
                   ESet p l r          -> ESet p <$> g l <*> g r
                   EBinOp p op l r     -> EBinOp p op <$> g l <*> g r
                   EUnOp p op v        -> EUnOp p op <$> g v
                   EPHolder p          -> return $ EPHolder p
                   EBinding p v x      -> EBinding p v <$> g x
                   ETyped p x t        -> (\x' -> ETyped p x' t) <$> g x
                   EAs p x t           -> (\x' -> EAs p x' t) <$> g x
                   ERef p x            -> ERef p <$> g x


exprMap :: (a -> b) -> ExprNode a -> ExprNode b
exprMap f e = runIdentity $ exprMapM (\e' -> return $ f e') e

exprFoldCtx :: (ECtx -> ExprNode b -> b) -> ECtx -> Expr -> b
exprFoldCtx f ctx e = runIdentity $ exprFoldCtxM (\ctx' e' -> return $ f ctx' e') ctx e

exprFoldM :: (Monad m) => (ExprNode b -> m b) -> Expr -> m b
exprFoldM f e = exprFoldCtxM (\_ e' -> f e') undefined e

exprFold :: (ExprNode b -> b) -> Expr -> b
exprFold f e = runIdentity $ exprFoldM (return . f) e

exprTraverseCtxWithM :: (Monad m) => (ECtx -> ExprNode a -> m a) -> (ECtx -> ExprNode a -> m ()) -> ECtx -> Expr -> m ()
exprTraverseCtxWithM g f ctx e = do {_ <- exprFoldCtxM (\ctx' e' -> do {f ctx' e'; g ctx' e'}) ctx e; return ()}

exprTraverseCtxM :: (Monad m) => (ECtx -> ENode -> m ()) -> ECtx -> Expr -> m ()
exprTraverseCtxM = exprTraverseCtxWithM (\_ x -> return $ E x)

exprTraverseM :: (Monad m) => (ENode -> m ()) -> Expr -> m ()
exprTraverseM f = exprTraverseCtxM (\_ x -> f x) undefined

exprCollectCtxM :: (Monad m) => (ECtx -> ExprNode b -> m b) -> (b -> b -> b) -> ECtx -> Expr -> m b
exprCollectCtxM f op ctx e = exprFoldCtxM g ctx e
    where g ctx' x = do x' <- f ctx' x
                        return $ case x of
                                     EVar _ _              -> x'
                                     EApply _ _ as         -> foldl' op x' as
                                     EField _ s _          -> x' `op` s
                                     ETupField _ s _       -> x' `op` s
                                     EBool _ _             -> x'
                                     EInt _ _              -> x'
                                     EString _ _           -> x'
                                     EBit _ _ _            -> x'
                                     ESigned _ _ _         -> x'
                                     EStruct _ _ fs        -> foldl' (\a (_, _x) -> op a _x) x' fs
                                     ETuple _ fs           -> foldl' op x' fs
                                     ESlice _ v _ _        -> x' `op` v
                                     EMatch _ m cs         -> foldl' (\a (p,v) -> a `op` p `op` v) (x' `op` m) cs
                                     EVarDecl _ _          -> x'
                                     ESeq _ l r            -> x' `op` l `op` r
                                     EITE _ i t el         -> x' `op` i `op` t `op` el
                                     EFor _ _ i b          -> x' `op` i `op` b
                                     ESet _ l r            -> x' `op` l `op` r
                                     EBinOp _ _ l r        -> x' `op` l `op` r
                                     EUnOp _ _ v           -> x' `op` v
                                     EPHolder _            -> x'
                                     EBinding _ _ pat      -> x' `op` pat
                                     ETyped _ v _          -> x' `op` v
                                     EAs _ v _             -> x' `op` v
                                     ERef _ v              -> x' `op` v

exprCollectM :: (Monad m) => (ExprNode b -> m b) -> (b -> b -> b) -> Expr -> m b
exprCollectM f op e = exprCollectCtxM (\_ e' -> f e') op undefined e

exprCollectCtx :: (ECtx -> ExprNode b -> b) -> (b -> b -> b) -> ECtx -> Expr -> b
exprCollectCtx f op ctx e = runIdentity $ exprCollectCtxM (\ctx' x -> return $ f ctx' x) op ctx e

exprCollect :: (ExprNode b -> b) -> (b -> b -> b) -> Expr -> b
exprCollect f op e = runIdentity $ exprCollectM (return . f) op e

-- enumerate all variable occurrences in the expression
exprVarOccurrences :: ECtx -> Expr -> [(String, ECtx)]
exprVarOccurrences ctx e = exprCollectCtx (\ctx' e' ->
                                            case e' of
                                                 EVar _ v -> [(v, ctx')]
                                                 _        -> [])
                                          (++) ctx e

-- enumerate all variables that occur in the expression
exprVars :: Expr -> [String]
exprVars e = nub $ exprCollect (\case
                                EVar _ v -> [v]
                                _        -> [])
                               (++) e

-- | Free variables, i.e., variables that are used in the expression, but declared
-- outside of it
exprFreeVars :: DatalogProgram -> ECtx -> Expr -> [String]
exprFreeVars d ctx e = visible_vars `intersect` used_vars
    where
    visible_vars = map name $ ctxAllVars d ctx
    used_vars = exprVars e

-- True if expression evaluates to a constant
-- Note: this does not guarantee that the expression can be evaluated at compile
-- time.  It may contain a call to an external function, which cannot be
-- evaluated in Haskell.
exprIsConst :: DatalogProgram -> ECtx -> Expr -> Bool
exprIsConst d ctx e = null $ exprFreeVars d ctx e

-- Variables declared inside expression, visible in the code that follows the expression
exprVarDecls :: ECtx -> Expr -> [(String, ECtx)]
exprVarDecls ctx e =
    exprFoldCtx (\ctx' e' ->
                  case e' of
                       EStruct _ _ fs   -> concatMap snd fs
                       ETuple _ fs      -> concat fs
                       EVarDecl _ v     -> [(v, ctx')]
                       ESet _ l _       -> l
                       EBinding _ v e'' -> (v, ctx') : e''
                       ETyped _ e'' _   -> e''
                       _                -> []) ctx e

-- Non-recursively enumerate all functions invoked by the expression
exprFuncs :: Expr -> [String]
exprFuncs e = exprFuncs' [] e

exprFuncs' :: [String] -> Expr -> [String]
exprFuncs' acc e = nub $ exprCollect (\case
                                       EApply _ f _ -> if' (elem f acc) [] [f]
                                       _            -> [])
                                     (++) e

-- Recursively enumerate all functions invoked by the expression
exprFuncsRec :: DatalogProgram -> Expr -> [String]
exprFuncsRec d e = exprFuncsRec' d [] e

exprFuncsRec' :: DatalogProgram -> [String] -> Expr -> [String]
exprFuncsRec' d acc e =
    let new = exprFuncs' acc e in
    new ++ foldl' (\acc' f -> maybe acc' ((acc'++) . exprFuncsRec' d (acc++new++acc')) $ funcDef $ getFunc d f) [] new

isLVar :: DatalogProgram -> ECtx -> String -> Bool
isLVar d ctx v = isJust $ find ((==v) . name) $ fst $ ctxVars d ctx


-- | We support three kinds of patterns used in different contexts:
--
-- * Deconstruction patterns are used in left-hand side of
-- assignments that simultaneously deconstruct a value and bind its
-- fields to fresh variables.  They are built out of variable declarations,
-- tuples, placeholders, constructors, type annotations.  Types with multiple
-- constructors cannot be deconstructed in this manner.
--
-- * Field expression: variables, fields, and type annotations.
--
-- * Match patterns are used in match expressions, relational
-- atoms, and in assignment terms in rules. They simultaneously restrict the
-- structure of a value and bind its fields to fresh variables.  They are
-- built out of variable declarations (optionally omitting the 'var' keyword),
-- tuples, constructors, placeholders, constant values, and type annotations.

-- | True if expression can be interpreted as a match pattern.
exprIsPattern :: Expr -> Bool
exprIsPattern e = exprFold exprIsPattern' e

exprIsPattern' :: ExprNode Bool -> Bool
exprIsPattern' EString{}        = True
exprIsPattern' EBit{}           = True
exprIsPattern' ESigned{}        = True
exprIsPattern' EBool{}          = True
exprIsPattern' EInt{}           = True
exprIsPattern' EVarDecl{}       = True
exprIsPattern' (ETuple _ as)    = and as
exprIsPattern' (EStruct _ _ as) = all snd as
exprIsPattern' EPHolder{}       = True
exprIsPattern' (ETyped _ e _)   = e
exprIsPattern' (ERef _ e)       = e
exprIsPattern' (EBinding _ _ e) = e
exprIsPattern' _                = False

-- | Like 'exprIsPattern', but matches implicit variable declarations.
exprIsPatternImpl :: Expr -> Bool
exprIsPatternImpl e = exprFold exprIsPatternImpl' e

exprIsPatternImpl' :: ExprNode Bool -> Bool
exprIsPatternImpl' EString{}        = True
exprIsPatternImpl' EBit{}           = True
exprIsPatternImpl' ESigned{}        = True
exprIsPatternImpl' EBool{}          = True
exprIsPatternImpl' EInt{}           = True
exprIsPatternImpl' EVar{}           = True
exprIsPatternImpl' (ETuple _ as)    = and as
exprIsPatternImpl' (EStruct _ _ as) = all snd as
exprIsPatternImpl' EPHolder{}       = True
exprIsPatternImpl' (ETyped _ e _)   = e
exprIsPatternImpl' (ERef _ e)       = e
exprIsPatternImpl' (EBinding _ _ e) = e
exprIsPatternImpl' _                = False

-- | True if 'e' contains a placeholder ('_')
exprContainsPHolders :: Expr -> Bool
exprContainsPHolders e =
    exprCollect (\case
                  EPHolder _ -> True
                  _          -> False)
                (||) e

-- | True if 'e' is a deconstruction expression.
exprIsDeconstruct :: DatalogProgram -> Expr -> Bool
exprIsDeconstruct d e = exprFold (exprIsDeconstruct' d) e

exprIsDeconstruct' :: DatalogProgram -> ExprNode Bool -> Bool
exprIsDeconstruct' _ EVarDecl{}       = True
exprIsDeconstruct' _ (ETuple _ as)    = and as
exprIsDeconstruct' d (EStruct _ c as) = all snd as && consIsUnique d c
exprIsDeconstruct' _ EPHolder{}       = True
exprIsDeconstruct' _ (ETyped _ e _)   = e
exprIsDeconstruct' _ _                = False

-- | True if 'e' is a variable or field expression, and
-- can be assigned to (i.e., the variable is writable)
exprIsVarOrFieldLVal :: DatalogProgram -> ECtx -> Expr -> Bool
exprIsVarOrFieldLVal d ctx e = exprFoldCtx (exprIsVarOrFieldLVal' d) ctx e

exprIsVarOrFieldLVal' :: DatalogProgram -> ECtx -> ExprNode Bool -> Bool
exprIsVarOrFieldLVal' d ctx (EVar _ v) = isLVar d ctx v
exprIsVarOrFieldLVal' _ _   (EField _ e _)   = e
exprIsVarOrFieldLVal' _ _   (ETupField _ e _)= e
exprIsVarOrFieldLVal' _ _   (ETyped _ e _)   = e
exprIsVarOrFieldLVal' _ _   _                = False

-- | True if 'e' is a variable or field expression
exprIsVarOrField :: Expr -> Bool
exprIsVarOrField e = exprFold exprIsVarOrField' e

exprIsVarOrField' :: ExprNode Bool -> Bool
exprIsVarOrField' (EVar _ _)       = True
exprIsVarOrField' (EField _ e _)   = e
exprIsVarOrField' (ETupField _ e _)= e
exprIsVarOrField' (ETyped _ e _)   = e
exprIsVarOrField' _                = False

-- | Expression maps distinct assignments to input variables 'vs'
-- to distinct outputs.
exprIsInjective :: DatalogProgram -> ECtx -> S.Set String -> Expr -> Bool
exprIsInjective d ctx vs e =
    S.fromList (exprFreeVars d ctx e) == vs &&
    exprFold (exprIsInjective' d) e

-- No clever analysis here; just the obvious cases.
exprIsInjective' :: DatalogProgram -> ExprNode Bool -> Bool
exprIsInjective' _ EVar{}        = True
exprIsInjective' d EApply{..}    =
    -- FIXME: once we add support for recursive functions, be careful to avoid
    -- infinite recursion.  The simple thing to do is just to return False for
    -- recursive functions, as reasoning about them seems tricky otherwise.
    and exprArgs && (maybe False (exprIsInjective d (CtxFunc f) (S.fromList $ map name funcArgs)) $ funcDef)
    where f@Function{..} = getFunc d exprFunc
exprIsInjective' _ EBool{}       = True
exprIsInjective' _ EInt{}        = True
exprIsInjective' _ EString{}     = True
exprIsInjective' _ EBit{}        = True
exprIsInjective' _ ESigned{}     = True
exprIsInjective' _ EStruct{..}   = all snd exprStructFields
exprIsInjective' _ ETuple{..}    = and exprTupleFields
exprIsInjective' _ EUnOp{..}     = (elem exprUOp [Not, BNeg, UMinus]) && exprOp
exprIsInjective' _ ETyped{..}    = exprExpr
exprIsInjective' _ _             = False

-- | Transform types referenced in the expression
exprTypeMapM :: (Monad m) => (Type -> m Type) -> Expr -> m Expr
exprTypeMapM fun e = exprFoldM fun' e
    where
    fun' (ETyped p e' t) = (E . ETyped p e') <$> typeMapM fun t
    fun' (EAs p e' t)    = (E . EAs p e') <$> typeMapM fun t
    fun' e'              = return $ E e'


exprValidate :: (MonadError String me) => DatalogProgram -> [String] -> ECtx -> Expr -> me ()
exprValidate d tvars ctx e = {-trace ("exprValidate " ++ show e ++ " in \n" ++ show ctx) $ -} do
    exprTraverseCtxM (exprValidate1 d tvars) ctx e
    exprTraverseTypeME d (exprValidate2 d) ctx e
    exprTraverseCtxM (exprCheckMatchPatterns d) ctx e

-- This function does not perform type checking: just checks that all functions and
-- variables are defined; the number of arguments matches declarations, etc.
exprValidate1 :: (MonadError String me) => DatalogProgram -> [String] -> ECtx -> ExprNode Expr -> me ()
exprValidate1 _ _ ctx EVar{..} | ctxInRuleRHSPositivePattern ctx
                                          = return ()
exprValidate1 d _ ctx (EVar p v)          = do _ <- checkVar p d ctx v
                                               return ()
exprValidate1 d _ ctx (EApply p f as)     = do
    fun <- checkFunc p d f
    check (length as == length (funcArgs fun)) p
          "Number of arguments does not match function declaration"
    mapM_ (\(a, mut) -> when mut $ checkLExpr d ctx a)
          $ zip as (map argMut $ funcArgs fun)
exprValidate1 _ _ _   EField{}            = return ()
exprValidate1 _ _ _   ETupField{}         = return ()
exprValidate1 _ _ _   EBool{}             = return ()
exprValidate1 _ _ _   EInt{}              = return ()
exprValidate1 _ _ _   EString{}           = return ()
exprValidate1 _ _ _   EBit{}              = return ()
exprValidate1 _ _ _   ESigned{}           = return ()
exprValidate1 d _ ctx (EStruct p c _)     = do -- initial validation was performed by exprDesugar
    let tdef = consType d c
    case find ctxIsSetL $ ctxAncestors ctx of
         Nothing -> return ()
         Just ctx' -> when (not $ ctxIsRuleRCond $ ctxParent ctx') $ do
            check ((length $ typeCons $ fromJust $ tdefType tdef) == 1) p
                   $ "Type constructor in the left-hand side of an assignment is only allowed for types with one constructor, \
                     \ but \"" ++ name tdef ++ "\" has multiple constructors"
exprValidate1 _ _ _   ETuple{}            = return ()
exprValidate1 _ _ _   ESlice{}            = return ()
exprValidate1 _ _ _   EMatch{}            = return ()
exprValidate1 d _ ctx (EVarDecl p v)      = do
    check (ctxInSetL ctx || ctxInMatchPat ctx) p "Variable declaration is not allowed in this context"
    checkNoVar p d ctx v
{-                                     | otherwise
                                          = do checkNoVar p d ctx v
                                               check (ctxIsTyped ctx) p "Variable declared without a type"
                                               check (ctxIsSeq1 $ ctxParent ctx) p
                                                      "Variable declaration is not allowed in this context"-}
exprValidate1 _ _ _   ESeq{}              = return ()
exprValidate1 _ _ _   EITE{}              = return ()
exprValidate1 d _ ctx EFor{..}            = checkNoVar exprPos d ctx exprLoopVar
exprValidate1 d _ ctx (ESet _ l _)        = checkLExpr d ctx l
exprValidate1 _ _ _   EBinOp{}            = return ()
exprValidate1 _ _ _   EUnOp{}             = return ()

exprValidate1 _ _ ctx (EPHolder p)        = do
    let msg = case ctx of
                   CtxStruct EStruct{..} _ f -> "Missing field " ++ f ++ " in constructor " ++ exprConstructor
                   _               -> "_ is not allowed in this context"
    check (ctxPHolderAllowed ctx) p msg
exprValidate1 d _ ctx (EBinding p v _)    = do
    checkNoVar p d ctx v

exprValidate1 d tvs _ (ETyped _ _ t)      = typeValidate d tvs t
exprValidate1 d tvs _ (EAs _ _ t)         = typeValidate d tvs t
exprValidate1 _ _ ctx (ERef p _)          =
    -- Rust does not allow pattern matching inside 'Arc'
    check (ctxInRuleRHSPattern ctx || ctxInIndex ctx) p "Dereference pattern not allowed in this context"

-- True if a placeholder ("_") can appear in this context
ctxPHolderAllowed :: ECtx -> Bool
ctxPHolderAllowed ctx =
    case ctx of
         CtxSetL{}        -> True
         CtxTyped{}       -> pres
         CtxRuleRAtom{..} -> True
         CtxStruct{}      -> pres
         CtxTuple{}       -> pres
         CtxMatchPat{}    -> True
         CtxBinding{}     -> True
         CtxRef{}         -> True
         CtxIndex{}       -> True
         _                -> False
    where
    par = ctxParent ctx
    pres = ctxPHolderAllowed par

checkNoVar :: (MonadError String me) => Pos -> DatalogProgram -> ECtx -> String -> me ()
checkNoVar p d ctx v = check (isNothing $ lookupVar d ctx v) p
                              $ "Variable " ++ v ++ " already defined in this scope"

-- Traverse again with types.  This pass ensures that all sub-expressions
-- have well-defined types that match their context
exprTraverseTypeME :: (MonadError String me) => DatalogProgram -> (ECtx -> ExprNode Type -> me ()) -> ECtx -> Expr -> me ()
exprTraverseTypeME d = exprTraverseCtxWithM (\ctx e -> do
    --trace ("exprTraverseTypeME " ++ show ctx ++ "\n    " ++ show e) $ return ()
    t <- exprNodeType d ctx e
    case ctxExpectType d ctx of
         Nothing -> return ()
         Just t' -> check (typesMatch d t t') (pos e)
                          $ "Couldn't match expected type " ++ show t' ++ " with actual type " ++ show t ++ " (context: " ++ show ctx ++ ")"
    return t)

exprValidate2 :: (MonadError String me) => DatalogProgram -> ECtx -> ExprNode Type -> me ()
exprValidate2 d _   (ESlice p e h l)    =
    case typ' d e of
        TBit _ w -> do check (h >= l) p
                           $ "Upper bound of the slice must be greater than lower bound"
                       check (h < w) p
                           $ "Upper bound of the slice cannot exceed argument width"
        _        -> err (pos e) $ "Expression is not a bit vector"

exprValidate2 d _   (EMatch _ _ cs)     = do
    let t = snd $ head cs
    mapM_ ((\e -> checkTypesMatch (pos e) d t e) . snd) cs

exprValidate2 d _   (EBinOp p op e1 e2) = do
    case op of
        Eq     -> m
        Neq    -> m
        Lt     -> do {m; isint1}
        Gt     -> do {m; isint1}
        Lte    -> do {m; isint1}
        Gte    -> do {m; isint1}
        And    -> do {m; isbool}
        Or     -> do {m; isbool}
        Impl   -> do {m; isbool}
        Plus   -> do {m; isint1}
        Minus  -> do {m; isint1}
        ShiftR -> do isint1
        ShiftL -> do isint1
        Mod    -> do {m; isint1; isint2}
        Times  -> do {m; isint1; isint2}
        Div    -> do {m; isint1; isint2}
        BAnd   -> do {m; isbitOrSigned1}
        BOr    -> do {m; isbitOrSigned1}
        BXor   -> do {m; isbitOrSigned1}
        Concat | isString d e1
               -> return ()
        Concat -> do {isbit1; isbit2}
    where m = checkTypesMatch p d e1 e2
          isint1 = check (isInt d e1 || isBit d e1 || isSigned d e1) (pos e1) "Not an integer"
          isint2 = check (isInt d e2 || isBit d e2 || isSigned d e2) (pos e2) "Not an integer"
          isbit1 = check (isBit d e1) (pos e1) "Not a bit vector"
          isbitOrSigned1 = check (isBit d e1 || isSigned d e1) (pos e1) "Not a bit<> or signed<> value"
          isbit2 = check (isBit d e2) (pos e2) "Not a bit vector"
          isbool = check (isBool d e1) (pos e1) "Not a Boolean"

exprValidate2 d _   (EUnOp _ BNeg e)    =
    check (isBit d e || isSigned d e) (pos e) "Not a bit vector"
exprValidate2 d _   (EUnOp _ UMinus e)    =
    check (isSigned d e || isInt d e) (pos e)
        $ "Cannot negate expression of type " ++ show e ++ ". Negation applies to signed<> and bigint values only."
--exprValidate2 d ctx (EVarDecl p x)      = check (isJust $ ctxExpectType d ctx) p
--                                                 $ "Cannot determine type of variable " ++ x -- Context: " ++ show ctx
exprValidate2 d _   (EITE p _ t e)       = checkTypesMatch p d t e
exprValidate2 d _   (EFor p _ i _)       = checkIterable "iterator" p d i
exprValidate2 d _   (EAs p e t)          = do
    check (isBit d e || isSigned d e) p
        $ "Cannot type-cast expression of type " ++ show e ++ ".  The type-cast operator is only supported for bit<> and signed<> types."
    check (isBit d t || isSigned d t || isInt d t) p
        $ "Cannot type-cast expression to " ++ show t ++ ".  Only bit<>, signed<>, and bigint types can be cast to."
    when (not $ isInt d t) $
        check (isBit d e == isBit d t || typeWidth e' == typeWidth t') p $
            "Conversion between signed and unsigned bit vectors only supported across types of the same bit width. " ++
            "Try casting to " ++ show (t'{typeWidth = typeWidth e'}) ++ " first."
    where
    e' = typ' d e
    t' = typ' d t
exprValidate2 _ _   _                    = return ()

checkLExpr :: (MonadError String me) => DatalogProgram -> ECtx -> Expr -> me ()
checkLExpr d ctx e | ctxIsRuleRCond ctx =
    check (exprIsPattern e) (pos e)
        $ "Left-hand side of an assignment term can only contain variable declarations, type constructors, and tuples"
                   | otherwise =
    check (exprIsVarOrFieldLVal d ctx e || exprIsDeconstruct d e) (pos e)
        $ "Expression " ++ show e ++ " is not an l-value" -- in context " ++ show ctx

exprCheckMatchPatterns :: (MonadError String me) => DatalogProgram -> ECtx -> ExprNode Expr -> me ()
exprCheckMatchPatterns d ctx e@(EMatch _ x cs) = do
    let t = exprType d (CtxMatchExpr e ctx) x
        ct0 = typeConsTree t
    ct <- foldM (\ct pat -> do let (leftover, abducted) = consTreeAbduct d ct pat
                               check (not $ consTreeEmpty abducted) (pos pat)
                                      "Unsatisfiable match pattern"
                               return leftover)
                ct0 (map fst cs)
    check (consTreeEmpty ct) (pos x) "Non-exhaustive match patterns"

exprCheckMatchPatterns _ _   _               = return ()
