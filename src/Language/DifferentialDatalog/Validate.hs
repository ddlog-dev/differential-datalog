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

{-# LANGUAGE RecordWildCards, FlexibleContexts, LambdaCase, TupleSections #-}

module Language.DifferentialDatalog.Validate (
    validate) where

import qualified Data.Map as M
import Control.Monad.Except
import Data.Maybe
import Data.List
import Data.Char
import qualified Data.Graph.Inductive as G
--import Debug.Trace

import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.Ops
import Language.DifferentialDatalog.Expr
import Language.DifferentialDatalog.DatalogProgram
import Language.DifferentialDatalog.Rule
import Language.DifferentialDatalog.Index
import Language.DifferentialDatalog.Relation

bUILTIN_2STRING_FUNC :: String
bUILTIN_2STRING_FUNC = "std.__builtin_2string"

tOSTRING_FUNC_SUFFIX :: String
tOSTRING_FUNC_SUFFIX = "2string"

-- | Validate Datalog program
validate :: (MonadError String me) => DatalogProgram -> me DatalogProgram
validate d = do
    uniqNames ("Multiple definitions of constructor " ++)
              $ progConstructors d
    -- Validate typedef's
    mapM_ (typedefValidate d) $ M.elems $ progTypedefs d
    -- No cyclic dependencies between user-defined types
    checkAcyclicTypes d
    -- Desugar.  Must be called after typeValidate.
    d' <- progDesugar d
    -- Validate function prototypes
    mapM_ (funcValidateProto d') $ M.elems $ progFunctions d'
    -- Validate function implementations
    mapM_ (funcValidateDefinition d') $ M.elems $ progFunctions d'
    -- Validate relation declarations
    mapM_ (relValidate d') $ M.elems $ progRelations d'
    -- Validate indexes
    mapM_ (indexValidate d') $ M.elems $ progIndexes d'
    -- Validate rules
    mapM_ (ruleValidate d') $ progRules d'
    -- Validate transformers
    mapM_ (transformerValidate d') $ progTransformers d'
    -- Validate transformer applications
    mapM_ (applyValidate d') $ progApplys d'
    -- Validate dependency graph
    depGraphValidate d'
    -- Insert string conversion functions
    d'' <- progInjectStringConversions d'
    -- Convert 'int' constants to 'bit<>'.
    let d''' = progConvertIntsToBVs d''
    -- This check must be done after 'depGraphValidate', which may
    -- introduce recursion
    checkNoRecursion d'''
    return d'''

--    mapM_ (relValidate2 r)   refineRels
--    maybe (return ())
--          (\cyc -> errR r (pos $ getRelation r $ snd $ head cyc)
--                     $ "Dependency cycle among relations: " ++ (intercalate ", " $ map (name . snd) cyc))
--          $ (grCycle $ relGraph r)
--    mapM_ (relValidate3 r)   refineRels
--    validateFinal r

-- Reject program with recursion
checkNoRecursion :: (MonadError String me) => DatalogProgram -> me ()
checkNoRecursion d = do
    case grCycle (funcGraph d) of
         Nothing -> return ()
         Just t  -> err (pos $ getFunc d $ snd $ head t)
                        $ "Recursive function definition: " ++ (intercalate "->" $ map (name . snd) t)


funcGraph :: DatalogProgram -> G.Gr String ()
funcGraph DatalogProgram{..} =
    let g0 = foldl' (\g (i,f) -> G.insNode (i,f) g)
                    G.empty $ zip [0..] (M.keys progFunctions) in
    foldl' (\g (i,f) -> case funcDef f of
                             Nothing -> g
                             Just e  -> foldl' (\g' f' -> G.insEdge (i, M.findIndex f' progFunctions, ()) g')
                                               g (exprFuncs e))
           g0 $ zip [0..] $ M.elems progFunctions

-- Remove syntactic sugar
progDesugar :: (MonadError String me) => DatalogProgram -> me DatalogProgram
progDesugar d = progExprMapCtxM d (exprDesugar d)

-- Desugar expressions: convert all type constructor calls to named
-- field syntax.
-- Precondition: typedefs must be validated before calling this
-- function.
exprDesugar :: (MonadError String me) => DatalogProgram -> ECtx -> ENode -> me Expr
exprDesugar d _ e =
    case e of
         EStruct p c as -> do
            cons@Constructor{..} <- checkConstructor p d c
            let desugarPos = do
                    check (length as == length consArgs) p
                           $ "Number of arguments does not match constructor declaration: " ++ show cons
                    return $ zip (map name consArgs) (map snd as)
            let desugarNamed = do
                    uniq' (\_ -> p) id ("Multiple occurrences of a field " ++) $ map fst as
                    mapM_ (\(n,e') -> check (isJust $ find ((==n) . name) consArgs) (pos e')
                                           $ "Unknown field " ++ n) as
                    return $ map (\f -> (name f, maybe (E $ EPHolder p) id $ lookup (name f) as)) consArgs
            as' <- case as of
                        [] | null consArgs
                           -> return as
                        [] -> desugarNamed
                        _  | all (null . fst) as
                           -> desugarPos
                        _  | any (null . fst) as
                           -> err (pos e) $ "Expression mixes named and positional arguments to type constructor " ++ c
                        _  -> desugarNamed
            return $ E e{exprStructFields = as'}
         _              -> return $ E e

typedefValidate :: (MonadError String me) => DatalogProgram -> TypeDef -> me ()
typedefValidate d@DatalogProgram{..} tdef@TypeDef{..} = do
    uniq' (\_ -> tdefPos) id ("Multiple definitions of type argument " ++) tdefArgs
    mapM_ (\a -> check (M.notMember a progTypedefs) tdefPos
                        $ "Type argument " ++ a ++ " conflicts with user-defined type name")
          tdefArgs
    case tdefType of
         Nothing -> return ()
         Just t  -> do
             typeValidate d tdefArgs t
             let dif = tdefArgs \\ typeTypeVars t
             check (null dif) tdefPos
                    $ "The following type variables are not used in type definition: " ++ intercalate "," dif
    uniqNames ("Multiple definitions of attribute " ++) tdefAttrs
    mapM_ (typedefValidateAttr d tdef) tdefAttrs

typedefValidateAttr :: (MonadError String me) => DatalogProgram -> TypeDef -> Attribute -> me ()
typedefValidateAttr _ tdef@TypeDef{..} attr = do
    case name attr of
         "size" -> do
            check (isNothing tdefType) (pos attr)
                $ "Only extern types can have a \"size\" attribute"
            _ <- tdefCheckSizeAttr tdef
            return ()
         n -> err (pos attr) $ "Unknown attribute " ++ n

checkAcyclicTypes :: (MonadError String me) => DatalogProgram -> me ()
checkAcyclicTypes DatalogProgram{..} = do
    let g0 :: G.Gr String ()
        g0 = G.insNodes (mapIdx (\(t,_) i -> (i, t)) $ M.toList progTypedefs) G.empty
        typIdx t = M.findIndex t progTypedefs
        gfull = M.foldlWithKey (\g tn tdef ->
                                 foldl' (\g' t' -> G.insEdge (typIdx tn, typIdx t', ()) g') g
                                        $ maybe [] typeUserTypes $ tdefType tdef)
                               g0 progTypedefs
    maybe (return ())
          (\cyc -> throwError $ "Mutually recursive types: " ++
                                (intercalate " -> " $ map snd cyc))
          $ grCycle gfull

funcValidateProto :: (MonadError String me) => DatalogProgram -> Function -> me ()
funcValidateProto d f@Function{..} = do
    uniqNames ("Multiple definitions of argument " ++) funcArgs
    let tvars = funcTypeVars f
    mapM_ (typeValidate d tvars . argType) funcArgs
    typeValidate d tvars funcType

funcValidateDefinition :: (MonadError String me) => DatalogProgram -> Function -> me ()
funcValidateDefinition d f@Function{..} = do
    case funcDef of
         Nothing  -> return ()
         Just def -> exprValidate d (funcTypeVars f) (CtxFunc f) def

relValidate :: (MonadError String me) => DatalogProgram -> Relation -> me ()
relValidate d rel@Relation{..} = do
    typeValidate d [] relType
    check (isNothing relPrimaryKey || relRole == RelInput) (pos rel)
        $ "Only input relations can be declared with a primary key"
    maybe (return ()) (exprValidate d [] (CtxKey rel) . keyExpr) relPrimaryKey

--relValidate2 :: (MonadError String me) => Refine -> Relation -> me ()
--relValidate2 r rel@Relation{..} = do
--    assertR r ((length $ filter isPrimaryKey relConstraints) <= 1) relPos $ "Multiple primary keys are not allowed"
--    mapM_ (constraintValidate r rel) relConstraints
--    maybe (return ()) (mapM_ (ruleValidate r rel)) relDef
--    maybe (return ()) (\rules -> assertR r (any (not . ruleIsRecursive rel) rules) relPos
--                                         "View must have at least one non-recursive rule") relDef

--relTypeValidate :: (MonadError String me) => Refine -> Relation -> Pos -> Type -> me ()
--relTypeValidate r rel p   TArray{}  = errR r p $ "Arrays are not allowed in relations (in relation " ++ name rel ++ ")"
--relTypeValidate r rel p   TTuple{}  = errR r p $ "Tuples are not allowed in relations (in relation " ++ name rel ++ ")"
--relTypeValidate r rel p   TOpaque{} = errR r p $ "Opaque columns are not allowed in relations (in relation " ++ name rel ++ ")"
--relTypeValidate r rel p   TInt{}    = errR r p $ "Arbitrary-precision integers are not allowed in relations (in relation " ++ name rel ++ ")"
--relTypeValidate _ _   _   TStruct{} = return ()
--relTypeValidate _ _   _   TUser{}   = return ()
--relTypeValidate _ _   _   _         = return ()
--
--relValidate3 :: (MonadError String me) => Refine -> Relation -> me ()
--relValidate3 r rel = do
--    let types = relTypes r rel
--    mapM_ (\t -> relTypeValidate r rel (pos t) t) types
--    maybe (return ())
--          (\cyc -> errR r (pos rel)
--                     $ "Dependency cycle among types used in relation " ++ name rel ++ ":\n" ++
--                      (intercalate "\n" $ map (show . snd) cyc))
--          $ grCycle $ typeGraph r types

-- | Validate relation transformer
-- * input and output argument names must be unique
-- * all return types must be relations
-- * relation names are upper-case, function names are lower-case
-- * validate higher-order types
transformerValidate :: (MonadError String me) => DatalogProgram -> Transformer -> me ()
transformerValidate d Transformer{..} = do
    uniqNames ("Multiple definitions of transformer argument " ++)  $ transInputs ++ transOutputs
    mapM_ (\o -> check (hotypeIsRelation $ hofType o) (pos o)
                       "A transformer can only output relations") transOutputs
    mapM_ (\o -> case hofType o of
                      HOTypeRelation{} -> check (isUpper $ head $ name o)       (pos o) "Relation name must start with an upper-case letter"
                      HOTypeFunction{} -> check (not $ isUpper $ head $ name o) (pos o) "Function name may not start with an upper-case letter"
          ) $ transInputs ++ transOutputs
    mapM_ (hotypeValidate d . hofType) $ transInputs ++ transOutputs

-- | Validate transformer application
-- * Transformer exists
-- * Inputs and outputs refer to valid functions and relations
-- * Input/output types match transformer declaration
-- * Outputs cannot be bound to input relations
-- * Outputs of a transformer cannot be used in the head of a rule
--   or as output of another transformer
applyValidate :: (MonadError String me) => DatalogProgram -> Apply -> me ()
applyValidate d a@Apply{..} = do
    trans@Transformer{..} <- checkTransformer (pos a) d applyTransformer
    check (length applyInputs == length transInputs) (pos a)
          $ "Transformer " ++ name trans ++ " expects " ++ show (length transInputs) ++ " input arguments, but" ++
            show (length applyInputs) ++ " arguments are specified"
    check (length applyOutputs == length transOutputs) (pos a)
          $ "Transformer " ++ name trans ++ " returns " ++ show (length transOutputs) ++ " outputs, but" ++
            show (length applyOutputs) ++ " outputs are provided"
    types <- mapM (\(decl, conc) ->
            case hofType decl of
                 HOTypeFunction{..} -> do
                     f@Function{..} <- checkFunc (pos a) d conc
                     -- FIXME: we don't have a proper unification checker; therefore insist on transformer arguments
                     -- using no type variables.
                     -- A proper unification checker should handle constraints of the form
                     -- '(exists T1 . forall T2 . E(T1,T2))', where 'T1' and 'T2' are lists of type arguments, and 'E' is
                     -- a conjunction of type congruence expressions.
                     check (null $ funcTypeVars f) (pos a)
                           $ "Generic function " ++ conc ++ " cannot be passed as an argument to relation transformer"
                     check (length hotArgs == length funcArgs) (pos a)
                           $ "Transformer " ++ name trans ++ " expects a function that takes " ++ show (length hotArgs) ++ " arguments " ++
                             " but function " ++ name f ++ " takes " ++ show (length funcArgs) ++ " arguments"
                     mapM_ (\(farg, carg) -> check (argMut farg == argMut carg) (pos a) $
                                             "Argument " ++ name farg ++ " of formal argument " ++ name decl ++ " of transformer " ++ name trans ++
                                             " differs in mutability from argument " ++ name carg ++ " of function " ++ name f)
                           $ zip hotArgs funcArgs
                     return $ (zip (map typ hotArgs) (map typ funcArgs)) ++ [(hotType, funcType)]
                 HOTypeRelation{..} -> do
                     rel <- checkRelation (pos a) d conc
                     return [(hotType, relType rel)]
                  ) $ zip (transInputs ++ transOutputs) (applyInputs ++ applyOutputs)
    bindings <- unifyTypes d (pos a) ("in transformer application " ++ show a) $ concat types
    mapM_ (\ta -> case M.lookup ta bindings of
                       Nothing -> err (pos a) $ "Unable to bind type argument '" ++ ta ++
                                                " to a concrete type in transformer application " ++ show a
                       Just _  -> return ())
          $ transformerTypeVars trans
    mapM_ (\o -> check (relRole (getRelation d o) /= RelInput) (pos a)
                 $ "Transformer output cannot be bound to input relation " ++ o
          ) applyOutputs
    -- Things will break if a relation is assigned by an 'Apply' in the top scope and then occurs inside a
    -- recursive fragment.  Keep things simple by disallowing this.
    -- If this proves an important limitation, it can be dropped and replaced with a program
    -- transformation that introduces a new relation for each 'Apply' output and a rule that
    -- concatenates it to the original output relation.  But for now it seems like a useful
    -- restriction
    mapM_ (\o -> check (null $ relRules d o) (pos a)
                       $ "Output of a transformer application may not occur in the head of a rule, but relation " ++ o ++
                          " occurs in the following rules\n" ++ (intercalate "\n" $ map show $ relRules d o))
          applyOutputs
    -- Likewise, to relax this, modify 'compileApplyNode' to concatenate transformer output to
    -- existing relation if it exists.
    mapM_ (\o -> check (length (relApplys d o) == 1) (pos a)
                       $ "Relation " ++ o ++ " occurs as output of multiple transformer applications")
          applyOutputs

hotypeValidate :: (MonadError String me) => DatalogProgram -> HOType -> me ()
hotypeValidate d HOTypeFunction{..} = do
    -- FIXME: hacky way to validate function type by converting it into a function.
    let f = Function hotPos "" hotArgs hotType Nothing
    funcValidateProto d f

hotypeValidate d HOTypeRelation{..} = typeValidate d (typeTypeVars hotType) hotType

-- | Check the following properties of a Datalog dependency graph:
--
--  * Linearity: A rule contains at most one RHS atom that is mutually
--  recursive with its head.
--  * Stratified negation: No loop contains a negative edge.
depGraphValidate :: (MonadError String me) => DatalogProgram -> me ()
depGraphValidate d@DatalogProgram{..} = do
    let g = progDependencyGraph d
    -- strongly connected components of the dependency graph
    let sccs = map (map (fromJust . G.lab g)) $ G.scc g
    -- maps relation name to SCC that contains this relation
    let sccmap = M.fromList
                 $ concat
                 $ mapIdx (\scc i -> mapMaybe (\case
                                           DepNodeRel rel -> Just (rel, i)
                                           _              -> Nothing) scc)
                   sccs
    -- Linearity
    {-mapM_ (\rl@Rule{..} ->
            mapM_ (\a ->
                    do let lscc = sccmap M.! (atomRelation a)
                       let rlits = filter ((== lscc) . (sccmap M.!) . atomRelation . rhsAtom)
                                   $ filter rhsIsLiteral ruleRHS
                       when (length rlits > 1)
                            $ err (pos rl)
                            $ "At most one relation in the right-hand side of a rule can be mutually recursive with its head. " ++
                              "The following RHS literals are mutually recursive with " ++ atomRelation a ++ ": " ++
                              intercalate ", " (map show rlits))
                  ruleLHS)
          progRules -}
    -- Stratified negation:
    -- * a relation may not recursively depend on its negation;
    -- * Apply nodes may not occur in recursive loops, as they are assumed to always introduce
    --   negative loops
    mapM_ (\rl@Rule{..} ->
            mapM_ (\a ->
                    do let lscc = sccmap M.! (atomRelation a)
                       mapM_ (\rhs -> err (pos rl)
                                          $ "Relation " ++ (atomRelation $ rhsAtom rhs) ++ " is mutually recursive with " ++ atomRelation a ++
                                            " and therefore cannot appear negated in this rule")
                             $ filter ((== lscc) . (sccmap M.!) . atomRelation . rhsAtom)
                             $ filter (not . rhsPolarity)
                             $ filter rhsIsLiteral ruleRHS)
                  ruleLHS)
          progRules
    mapM_ (\scc -> let anode = find depNodeIsApply scc in
                   case anode of
                        Just (DepNodeApply a) -> err (pos a)
                                                 $ "Transformer application appears in a recursive fragment consisting of the following relations: " ++
                                                 (show scc)
                        _ -> return ())
          $ filter ((> 1) . length) sccs

-- Automatically insert string conversion functions in the Concat
-- operator:  '"x:" ++ x', where 'x' is of type int becomes
-- '"x:" ++ int_2string(x)'.
progInjectStringConversions :: (MonadError String me) => DatalogProgram -> me DatalogProgram
progInjectStringConversions d = progExprMapCtxM d (exprInjectStringConversions d)

exprInjectStringConversions :: (MonadError String me) => DatalogProgram -> ECtx -> ENode -> me Expr
exprInjectStringConversions d ctx e@(EBinOp p Concat l r) | (te == tString) && (tr /= tString) = do
    -- find string conversion function
    fname <- case tr of
                  TBool{}     -> return $ bUILTIN_2STRING_FUNC
                  TInt{}      -> return $ bUILTIN_2STRING_FUNC
                  TString{}   -> return $ bUILTIN_2STRING_FUNC
                  TBit{}      -> return $ bUILTIN_2STRING_FUNC
                  TSigned{}   -> return $ bUILTIN_2STRING_FUNC
                  TUser{..}   -> return $ mk2string_func typeName
                  TOpaque{..} -> return $ mk2string_func typeName
                  TTuple{}    -> err (pos r) "Automatic string conversion for tuples is not supported"
                  TVar{..}    -> err (pos r) $
                                     "Cannot automatically convert " ++ show r ++
                                     " of variable type " ++ tvarName ++ " to string"
                  TStruct{}   -> error "unexpected TStruct in exprInjectStringConversions"
    f <- case lookupFunc d fname of
              Nothing  -> err (pos r) $ "Cannot find declaration of function " ++ fname ++
                                        " needed to convert expression " ++ show r ++ " to string"
              Just fun -> return fun
    let arg0 = funcArgs f !! 0
    -- validate its signature
    check (isString d $ funcType f) (pos f)
           "string conversion function must return \"string\""
    check ((length $ funcArgs f) == 1) (pos f)
           "string conversion function must take exactly one argument"
    _ <- unifyTypes d p
           ("in the call to string conversion function \"" ++ name f ++ "\"")
           [(typ arg0, tr)]
    let r' = E $ EApply (pos r) fname [r]
    return $ E $ EBinOp p Concat l r'
    where te = exprType'' d ctx $ E e
          tr = exprType'' d (CtxBinOpR e ctx) r
          mk2string_func cs = ((toLower $ head cs) : tail cs) ++ tOSTRING_FUNC_SUFFIX

exprInjectStringConversions _ _   e = return $ E e

progConvertIntsToBVs :: DatalogProgram -> DatalogProgram
progConvertIntsToBVs d = progExprMapCtx d (exprConvertIntToBV d)

exprConvertIntToBV :: DatalogProgram -> ECtx -> ENode -> Expr
exprConvertIntToBV d ctx e@(EInt p v) =
    case exprType' d ctx (E e) of
         TBit _ w    -> E $ EBit p w v
         TSigned _ w -> E $ ESigned p w v
         _           -> E e
exprConvertIntToBV _ _ e = E e
