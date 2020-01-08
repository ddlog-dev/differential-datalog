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

{-# LANGUAGE RecordWildCards, FlexibleContexts, LambdaCase, TupleSections, OverloadedStrings, TemplateHaskell, QuasiQuotes, ImplicitParams, NamedFieldPuns #-}

{- |
Module     : Compile
Description: Compile 'DatalogProgram' to Rust.  See program.rs for corresponding Rust declarations.
-}

module Language.DifferentialDatalog.Compile (
    CompilerConfig(..),
    defaultCompilerConfig,
    compile,
    rustProjectDir,
    mkValConstructorName,
    mkConstructorName,
    mkType,
    mkValue,
    rname
) where

import Prelude hiding((<>))
import Control.Monad.State
import Text.PrettyPrint
import Data.Tuple.Select
import Data.Maybe
import Data.List
import Data.Bits hiding (isSigned)
import Data.List.Split
import Data.FileEmbed
import Data.String.Utils
import System.FilePath
import System.Directory
import qualified Data.ByteString.Char8 as BS
import Numeric
import qualified Data.Set as S
import qualified Data.Map as M
import qualified Data.Graph.Inductive as G
import Data.WideWord
--import Debug.Trace

import Language.DifferentialDatalog.PP
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Ops
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Parse
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Expr
import Language.DifferentialDatalog.DatalogProgram
import Language.DifferentialDatalog.Relation
import Language.DifferentialDatalog.Index
import Language.DifferentialDatalog.Optimize
import Language.DifferentialDatalog.ECtx
import Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.Rule
import Language.DifferentialDatalog.FlatBuffer

-- Input argument name for Rust functions that take a datalog record.
vALUE_VAR :: Doc
vALUE_VAR = "__v"

-- Input arguments for Rust functions that take a key and one or two
-- values
kEY_VAR :: Doc
kEY_VAR = "__key"

vALUE_VAR1 :: Doc
vALUE_VAR1 = "__v1"

vALUE_VAR2 :: Doc
vALUE_VAR2 = "__v2"

vALUE_VAR12 :: Doc
vALUE_VAR12 = "__v12"

-- Input argument to aggregation function
gROUP_VAR :: Doc
gROUP_VAR = "__group__"

-- Functions that return a Rust reference rather than a value.
-- FIXME: there should be a way to annotate function definitions
-- with this, but for now we only have one such function.
fUNCS_RETURN_REF :: [String]
fUNCS_RETURN_REF = ["std.deref"]

-- Extract static template header before the "/*- !!!!!!!!!!!!!!!!!!!! -*/"
-- separator from file; substitute "datalog_example" with 'specname' in
-- the header.
header :: String -> String -> Doc
header template specname =
    let h = case splitOn "/*- !!!!!!!!!!!!!!!!!!!! -*/" template of
                []  -> error "Missing separator in lib.rs"
                x:_ -> x
    in pp $ replace "datalog_example" specname h

-- 'types' crate containing DDlog type declarations.
typesHeader :: String -> Doc
typesHeader specname = header (BS.unpack $ $(embedFile "rust/template/types/lib.rs")) specname

-- 'value' crate that declares relations and value types.
valueHeader :: String -> Doc
valueHeader specname = header (BS.unpack $ $(embedFile "rust/template/value/lib.rs")) specname

-- Main crate containing compiled program rules.
mainHeader :: String -> Doc
mainHeader specname = header (BS.unpack $ $(embedFile "rust/template/src/lib.rs")) specname

-- Top-level 'Cargo.toml'.
mainCargo :: String -> [String] -> Doc
mainCargo specname crate_types =
    (pp $ replace "datalog_example" specname $ BS.unpack $ $(embedFile "rust/template/Cargo.toml")) $$
    "crate-type = [" <> (hsep $ punctuate "," $ map (\t -> "\"" <> pp t <> "\"") $ "rlib" : crate_types) <> "]"

-- 'types/Cargo.toml' - imports Rust dependencies.
typesCargo :: String -> Doc -> Doc
typesCargo specname toml_code =
    (pp $ replace "datalog_example" specname $ BS.unpack $ $(embedFile "rust/template/types/Cargo.toml")) $$
    "" $$ toml_code

rustProjectDir :: String -> String
rustProjectDir specname = specname ++ "_ddlog"

-- IMPORTANT: KEEP THIS IN SYNC WITH FILE LIST IN 'build.rs'.
templateFiles :: String -> [(String, String)]
templateFiles specname =
    map (mapSnd (BS.unpack)) $
        [ (dir </> "src/build.rs"               , $(embedFile "rust/template/src/build.rs"))
        , (dir </> "src/main.rs"                , $(embedFile "rust/template/src/main.rs"))
        , (dir </> "src/api.rs"                 , $(embedFile "rust/template/src/api.rs"))
        , (dir </> "src/ovsdb.rs"               , $(embedFile "rust/template/src/ovsdb.rs"))
        , (dir </> "src/update_handler.rs"      , $(embedFile "rust/template/src/update_handler.rs"))
        , (dir </> "ddlog.h"                    , $(embedFile "rust/template/ddlog.h"))
        , (dir </> "ddlog_ovsdb_test.c"         , $(embedFile "rust/template/ddlog_ovsdb_test.c"))
        ]
    where dir = rustProjectDir specname

-- Rust differential_datalog library
rustLibFiles :: String -> [(String, String)]
rustLibFiles specname =
    map (mapSnd (BS.unpack)) $
        [ (dir </> "differential_datalog/Cargo.toml"                 , $(embedFile "rust/template/differential_datalog/Cargo.toml"))
        , (dir </> "differential_datalog/arcval.rs"                  , $(embedFile "rust/template/differential_datalog/arcval.rs"))
        , (dir </> "differential_datalog/callback.rs"                , $(embedFile "rust/template/differential_datalog/callback.rs"))
        , (dir </> "differential_datalog/ddlog.rs"                   , $(embedFile "rust/template/differential_datalog/ddlog.rs"))
        , (dir </> "differential_datalog/ddval.rs"                   , $(embedFile "rust/template/differential_datalog/ddval.rs"))
        , (dir </> "differential_datalog/int.rs"                     , $(embedFile "rust/template/differential_datalog/int.rs"))
        , (dir </> "differential_datalog/lib.rs"                     , $(embedFile "rust/template/differential_datalog/lib.rs"))
        , (dir </> "differential_datalog/profile.rs"                 , $(embedFile "rust/template/differential_datalog/profile.rs"))
        , (dir </> "differential_datalog/program.rs"                 , $(embedFile "rust/template/differential_datalog/program.rs"))
        , (dir </> "differential_datalog/record.rs"                  , $(embedFile "rust/template/differential_datalog/record.rs"))
        , (dir </> "differential_datalog/replay.rs"                  , $(embedFile "rust/template/differential_datalog/replay.rs"))
        , (dir </> "differential_datalog/test.rs"                    , $(embedFile "rust/template/differential_datalog/test.rs"))
        , (dir </> "differential_datalog/test_value.rs"              , $(embedFile "rust/template/differential_datalog/test_value.rs"))
        , (dir </> "differential_datalog/test_record.rs"             , $(embedFile "rust/template/differential_datalog/test_record.rs"))
        , (dir </> "differential_datalog/uint.rs"                    , $(embedFile "rust/template/differential_datalog/uint.rs"))
        , (dir </> "differential_datalog/valmap.rs"                  , $(embedFile "rust/template/differential_datalog/valmap.rs"))
        , (dir </> "differential_datalog/variable.rs"                , $(embedFile "rust/template/differential_datalog/variable.rs"))
        , (dir </> "cmd_parser/Cargo.toml"                           , $(embedFile "rust/template/cmd_parser/Cargo.toml"))
        , (dir </> "cmd_parser/lib.rs"                               , $(embedFile "rust/template/cmd_parser/lib.rs"))
        , (dir </> "cmd_parser/parse.rs"                             , $(embedFile "rust/template/cmd_parser/parse.rs"))
        , (dir </> "distributed_datalog/Cargo.toml"                  , $(embedFile "rust/template/distributed_datalog/Cargo.toml"))
        , (dir </> "distributed_datalog/src/assign.rs"               , $(embedFile "rust/template/distributed_datalog/src/assign.rs"))
        , (dir </> "distributed_datalog/src/instantiate.rs"          , $(embedFile "rust/template/distributed_datalog/src/instantiate.rs"))
        , (dir </> "distributed_datalog/src/lib.rs"                  , $(embedFile "rust/template/distributed_datalog/src/lib.rs"))
        , (dir </> "distributed_datalog/src/observe/mod.rs"          , $(embedFile "rust/template/distributed_datalog/src/observe/mod.rs"))
        , (dir </> "distributed_datalog/src/observe/observable.rs"   , $(embedFile "rust/template/distributed_datalog/src/observe/observable.rs"))
        , (dir </> "distributed_datalog/src/observe/observer.rs"     , $(embedFile "rust/template/distributed_datalog/src/observe/observer.rs"))
        , (dir </> "distributed_datalog/src/observe/test.rs"         , $(embedFile "rust/template/distributed_datalog/src/observe/test.rs"))
        , (dir </> "distributed_datalog/src/read_config.rs"          , $(embedFile "rust/template/distributed_datalog/src/read_config.rs"))
        , (dir </> "distributed_datalog/src/schema.rs"               , $(embedFile "rust/template/distributed_datalog/src/schema.rs"))
        , (dir </> "distributed_datalog/src/server.rs"               , $(embedFile "rust/template/distributed_datalog/src/server.rs"))
        , (dir </> "distributed_datalog/src/sinks/file.rs"           , $(embedFile "rust/template/distributed_datalog/src/sinks/file.rs"))
        , (dir </> "distributed_datalog/src/sinks/mod.rs"            , $(embedFile "rust/template/distributed_datalog/src/sinks/mod.rs"))
        , (dir </> "distributed_datalog/src/sources/file.rs"         , $(embedFile "rust/template/distributed_datalog/src/sources/file.rs"))
        , (dir </> "distributed_datalog/src/sources/mod.rs"          , $(embedFile "rust/template/distributed_datalog/src/sources/mod.rs"))
        , (dir </> "distributed_datalog/src/tcp_channel/message.rs"  , $(embedFile "rust/template/distributed_datalog/src/tcp_channel/message.rs"))
        , (dir </> "distributed_datalog/src/tcp_channel/mod.rs"      , $(embedFile "rust/template/distributed_datalog/src/tcp_channel/mod.rs"))
        , (dir </> "distributed_datalog/src/tcp_channel/receiver.rs" , $(embedFile "rust/template/distributed_datalog/src/tcp_channel/receiver.rs"))
        , (dir </> "distributed_datalog/src/tcp_channel/sender.rs"   , $(embedFile "rust/template/distributed_datalog/src/tcp_channel/sender.rs"))
        , (dir </> "distributed_datalog/src/tcp_channel/socket.rs"   , $(embedFile "rust/template/distributed_datalog/src/tcp_channel/socket.rs"))
        , (dir </> "distributed_datalog/src/tcp_channel/txnbuf.rs"   , $(embedFile "rust/template/distributed_datalog/src/tcp_channel/txnbuf.rs"))
        , (dir </> "distributed_datalog/src/test.rs"                 , $(embedFile "rust/template/distributed_datalog/src/test.rs"))
        , (dir </> "distributed_datalog/src/txnmux.rs"               , $(embedFile "rust/template/distributed_datalog/src/txnmux.rs"))
        , (dir </> "distributed_datalog/src/zookeeper.rs"            , $(embedFile "rust/template/distributed_datalog/src/zookeeper.rs"))
        , (dir </> "ovsdb/Cargo.toml"                                , $(embedFile "rust/template/ovsdb/Cargo.toml"))
        , (dir </> "ovsdb/lib.rs"                                    , $(embedFile "rust/template/ovsdb/lib.rs"))
        , (dir </> "ovsdb/test.rs"                                   , $(embedFile "rust/template/ovsdb/test.rs"))
        , (dir </> "types/log.rs"                                    , $(embedFile "rust/template/types/log.rs"))
        , (dir </> "value/Cargo.toml"                                , $(embedFile "rust/template/value/Cargo.toml"))
        , (dir </> ".cargo/config"                                   , $(embedFile "rust/template/.cargo/config"))
        ]
    where dir = rustProjectDir specname


{- The following types model corresponding entities in program.rs -}

-- There are two kinds of arrangements:
--
-- + 'ArrangementMap' arranges the collection into key-value pairs of type
--   '(DDValue,DDValue)', where the second value in the pair is a record from the
--   original collection.  These arrangements are used in joins and semi-joins.
--
-- + 'ArrangementSet' arranges the collection in '(DDValue, ())' pairs, where the
--   value is the key extracted from relation.  These arrangements are used in
--   antijoins and semijoins.
--
-- A semijoin can use either 'ArrangementSet' or 'ArrangementMap'.  The latter is
-- more expensive and should only be used if the same arrangement is shared with a
-- join operator.
--
-- 'arngPattern' is a _normalized_ pattern that only contains variables involved
-- in the arrangement key, with normalized names (so that two patterns isomorphic
-- modulo variable names have the same normalized representation) and that only
-- expand constructors that either contain a key variable or are non-unique.
--
-- The 'distinct' flag in 'ArrangementSet' indicates that this arrangement is used
-- in an antijoin and therefore must contain distinct entries.
data Arrangement = ArrangementMap { arngPattern :: Expr, arngIndexes :: [Index] }
                 | ArrangementSet { arngPattern :: Expr, arngDistinct :: Bool}
                 deriving Eq

arngUsedInIndexes :: Arrangement -> [Index]
arngUsedInIndexes ArrangementMap{arngIndexes} = arngIndexes
arngUsedInIndexes ArrangementSet{} = []

arngIsDistinct :: Arrangement -> Bool
arngIsDistinct ArrangementMap{} = False
arngIsDistinct ArrangementSet{arngDistinct} = arngDistinct

-- Rust expression kind
data EKind = EVal         -- normal value
           | ELVal        -- l-value that can be written to or moved
           | EReference   -- reference (mutable or immutable)
           | ENoReturn    -- expression does not return (e.g., break, continue, or return).
           deriving (Eq, Show)

-- convert any expression into reference
ref :: (Doc, EKind, ENode) -> Doc
ref (x, EReference, _)  = x
ref (x, ENoReturn, _)   = x
ref (x, _, _)           = parens $ "&" <> x

-- dereference expression if it is a reference; leave it alone
-- otherwise
deref :: (Doc, EKind, ENode) -> Doc
deref (x, EReference, _) = parens $ "*" <> x
deref (x, _, _)          = x

-- convert any expression into mutable reference
mutref :: (Doc, EKind, ENode) -> Doc
mutref (x, EReference, _)  = x
mutref (x, ENoReturn, _)   = x
mutref (x, _, _)           = parens $ "&mut" <> x

-- convert any expression to EVal by cloning it if necessary
val :: (Doc, EKind, ENode) -> Doc
val (x, EVal, _)      = x
val (x, ENoReturn, _) = x
val (x, _, _)         = x <> ".clone()"

-- convert expression to l-value
lval :: (Doc, EKind, ENode) -> Doc
lval (x, ELVal, _)      = x
-- this can only be mutable reference in a valid program
lval (x, EReference, _) = parens $ "*" <> x
lval (x, EVal, _)       = error $ "Compile.lval: cannot convert value to l-value: " ++ show x
lval (x, ENoReturn, _)  = error $ "Compile.lval: cannot convert expression to l-value: " ++ show x

-- | 'CompilerConfig'
-- 'cconfJava' - generate Java bindings to the DDlog program
data CompilerConfig = CompilerConfig {
    cconfJava         :: Bool
}

defaultCompilerConfig :: CompilerConfig
defaultCompilerConfig = CompilerConfig {
    cconfJava = False
}

-- Compiled relation: Rust code for the 'struct Relation' plus ground facts for this relation.
data ProgRel = ProgRel {
    prelName    :: String,
    prelCode    :: Doc,
    prelFacts   :: [Doc]
}

-- Recursive relation contains additional flag that indicates whether the
-- `distinct` operator should be applied to the relation before each fixed
-- point iteration to ensure convergence.
data RecProgRel = RecProgRel {
    rprelRel      :: ProgRel,
    rprelDistinct :: Bool
}

-- Compiled program node: individual relation or a recursive fragment
data ProgNode = SCCNode   [RecProgRel]
              | ApplyNode Doc
              | RelNode   ProgRel

nodeRels :: ProgNode -> [ProgRel]
nodeRels (SCCNode rels) = map rprelRel rels
nodeRels (RelNode rel)  = [rel]
nodeRels (ApplyNode _)  = []

{- State accumulated by the compiler as it traverses the program -}
type CompilerMonad = State CompilerState

data CompilerState = CompilerState {
    cTypes        :: S.Set Type,
    cArrangements :: M.Map String [Arrangement]
}

emptyCompilerState :: CompilerState
emptyCompilerState = CompilerState {
    cTypes        = S.empty,
    cArrangements = M.empty
}

-- Convert name to a valid Rust identifier by replacing "." with "_"
rname :: String -> Doc
rname = pp . replace "." "_"

mkRelEnum :: DatalogProgram -> Doc
mkRelEnum d =
    "#[derive(Copy,Clone,Debug,PartialEq,Eq,Hash)]"                                                                                       $$
    "pub enum Relations {"                                                                                                                $$
    (nest' $ vcat $ punctuate comma $ map (\rel -> rname (name rel) <+> "=" <+> pp (relIdentifier d rel)) $ M.elems $ progRelations d)    $$
    "}"

mkIdxEnum :: DatalogProgram -> Doc
mkIdxEnum d =
    "#[derive(Copy,Clone,Debug,PartialEq,Eq,Hash)]"                                                                                       $$
    "pub enum Indexes {"                                                                                                                  $$
    (nest' $ vcat $ punctuate comma $ map (\idx -> rname (name idx) <+> "=" <+> pp (idxIdentifier d idx)) $ M.elems $ progIndexes d)      $$
    "}"

relId :: String -> Doc
relId rel = "Relations::" <> rname rel <+> "as RelId"

-- t must be normalized
addType :: Type -> CompilerMonad ()
addType t = modify $ \s -> s{cTypes = S.insert t $ cTypes s}

-- Create a new arrangement for use in a join operator:
-- * If the arrangement exists, do nothing
-- * If a semijoin arrangement with the same pattern exists,
--   promote it to a join arrangement
-- * Otherwise, add the new arrangement
addJoinArrangement :: String -> Expr -> Maybe Index -> CompilerMonad ()
addJoinArrangement relname pattern midx = do
    arrs <- gets $ (M.! relname) . cArrangements
    let existing_idx = findIndex (\a -> (arngPattern a == pattern) && (arngIsDistinct a == False)) arrs
    let idxs = nub $ maybeToList midx ++ maybe [] (arngUsedInIndexes . (arrs !!)) existing_idx
    let join_arr = ArrangementMap pattern idxs
    let arrs' = maybe (arrs ++ [join_arr])
                      (\idx -> take idx arrs ++ [join_arr] ++ drop (idx+1) arrs)
                      existing_idx
    modify $ \s -> s{cArrangements = M.insert relname arrs' $ cArrangements s}

-- Create a new arrangement for use in a semijoin operator:
-- * If a semijoin, antijoin or join arrangement with the same pattern exists, do nothing
-- * Otherwise, add the new arrangement
addSemijoinArrangement :: String -> Expr -> CompilerMonad ()
addSemijoinArrangement relname pattern = do
    arrs <- gets $ (M.! relname) . cArrangements
    let arrs' = if isJust $ find ((==pattern) . arngPattern) arrs
                   then arrs
                   else arrs ++ [ArrangementSet pattern False]
    modify $ \s -> s{cArrangements = M.insert relname arrs' $ cArrangements s}

-- Create a new arrangement for use in a antijoin operator:
-- * If the arrangement exists, do nothing
-- * If a semijoin arrangement with the same pattern exists, promote it to
--   an antijoin by setting 'distinct' to false
-- * Otherwise, add the new arrangement
addAntijoinArrangement :: String -> Expr -> CompilerMonad ()
addAntijoinArrangement relname pattern = do
    arrs <- gets $ (M.! relname) . cArrangements
    let antijoin_arr = ArrangementSet pattern True
    let semijoin_idx = elemIndex (ArrangementSet pattern False) arrs
    let arrs' = if elem antijoin_arr arrs
                   then arrs
                   else maybe (arrs ++ [antijoin_arr])
                              (\idx -> take idx arrs ++ [antijoin_arr] ++ drop (idx+1) arrs)
                              semijoin_idx
    modify $ \s -> s{cArrangements = M.insert relname arrs' $ cArrangements s}

-- Find an arrangement of the form 'ArrangementSet pattern _'
getSemijoinArrangement :: String -> Expr -> CompilerMonad (Maybe Int)
getSemijoinArrangement relname pattern = do
    arrs <- gets $ (M.! relname) . cArrangements
    return $ findIndex (\case
                         ArrangementSet pattern' _ -> pattern' == pattern
                         _                         -> False)
                       arrs

-- Find an arrangement of the form 'ArrangementMap pattern _'
getJoinArrangement :: String -> Expr -> CompilerMonad (Maybe Int)
getJoinArrangement relname pattern = do
    arrs <- gets $ (M.! relname) . cArrangements
    return $ findIndex (\case
                         ArrangementMap{arngPattern} -> arngPattern == pattern
                         _ -> False) arrs

-- Find an arrangement of the form 'ArrangementSet pattern True'
getAntijoinArrangement :: String -> Expr -> CompilerMonad (Maybe Int)
getAntijoinArrangement relname pattern = do
    arrs <- gets $ (M.! relname) . cArrangements
    return $ elemIndex (ArrangementSet pattern True) arrs

-- Rust does not like parenthesis around singleton tuples
tuple :: [Doc] -> Doc
tuple [x] = x
tuple xs = parens $ hsep $ punctuate comma xs

tupleTypeName :: [a] -> Doc
tupleTypeName xs = "tuple" <> pp (length xs)

-- Rust does not implement Eq and other traits for tuples with >12 elements.
-- Use structs with n fields for longer tuples.
tupleStruct :: [Doc] -> Doc
tupleStruct [x]                  = x
tupleStruct xs | length xs <= 12 = tuple xs
               | otherwise       = tupleTypeName xs <> tuple xs

-- Structs with a single constructor are compiled into Rust structs;
-- structs with multiple constructor are compiled into Rust enums.
isStructType :: Type -> Bool
isStructType TStruct{..} | length typeCons == 1 = True
isStructType TStruct{..}                        = False
isStructType t                                  = error $ "Compile.isStructType " ++ show t

mkConstructorName :: String -> Type -> String -> Doc
mkConstructorName tname t c =
    if isStructType t
       then rname tname
       else rname tname <> "::" <> rname c

-- | Create a compilable Cargo crate.  If the crate already exists, only writes files
-- modified by the recompilation.
--
-- 'specname' - will be used as Cargo package and library names
--
-- 'rs_code' - additional Rust code to be added to the generated program 'lib.rs'.
--
-- 'toml_code' - code to be added to the generated 'Cargo.toml' file
--
-- 'dir' - directory for the crate; will be created if does not exist
--
-- 'crate_types' - list of Cargo library crate types, e.g., [\"staticlib\"],
--                  [\"cdylib\"], [\"staticlib\", \"cdylib\"]
compile :: (?cfg::CompilerConfig) => DatalogProgram -> String -> Doc -> Doc -> FilePath -> [String] -> IO ()
compile d_unoptimized specname rs_code toml_code dir crate_types = do
    -- Create dir if it does not exist.
    createDirectoryIfMissing True (dir </> rustProjectDir specname)
    -- dump dependency graph to file
    updateFile (dir </> rustProjectDir specname </> specname <.> "dot")
               (depGraphToDot $ progDependencyGraph d_unoptimized)
    -- Apply optimizations; make sure the program has at least one relation.
    let d = addDummyRel $ optimize d_unoptimized
    let (types, value, main) = compileLib d specname rs_code
    when (cconfJava ?cfg) $
        compileFlatBufferBindings d specname (dir </> rustProjectDir specname)
    -- Substitute specname template files; write files if changed.
    mapM_ (\(path, content) -> do
            let path' = dir </> path
                content' = replace "datalog_example" specname content
            updateFile path' content')
          $ templateFiles specname
    -- Update rustLibFiles if they changed.
    mapM_ (\(path, content) -> do
            let path' = dir </> path
            updateFile path' content)
         $ rustLibFiles specname
    -- Generate lib files if changed.
    updateFile (dir </> rustProjectDir specname </> "types/Cargo.toml") (render $ typesCargo specname toml_code)
    updateFile (dir </> rustProjectDir specname </> "types/lib.rs")     (render types)
    updateFile (dir </> rustProjectDir specname </> "value/lib.rs")     (render value)
    updateFile (dir </> rustProjectDir specname </> "Cargo.toml")       (render $ mainCargo specname crate_types)
    updateFile (dir </> rustProjectDir specname </> "src/lib.rs")       (render main)
    return ()

-- | Compile Datalog program into Rust code.
--
-- Returns Rust code for three crates that comp:
-- * 'types' crate containing DDlog type declarations, functions, and all imported
--   Rust library code.
-- * 'value' crate that declares relations and value types.
-- * 'main' crate that contains rule definitions in Rust and imports the other two.
--
compileLib :: (?cfg::CompilerConfig) => DatalogProgram -> String -> Doc -> (Doc, Doc, Doc)
compileLib d specname rs_code = (typesLib, valueLib, mainLib)
    where
    typesLib = typesHeader specname         $+$
               rs_code                      $+$
               typedefs                     $+$
               funcs
    valueLib = valueHeader specname         $+$
               mkValType d (cTypes cstate)  $+$ -- 'Value' enum type
               mkValueFromRecord d          $+$ -- Function to convert cmd_parser::Record to Value
               mkIndexesIntoArrId d cstate  $+$
               mkRelEnum d                  $+$ -- 'enum Relations'
               mkIdxEnum d                      -- 'enum Indexes'
    mainLib = mainHeader specname           $+$
              prog
    -- Compute ordered SCCs of the dependency graph.  These will define the
    -- structure of the program.
    depgraph = progDependencyGraph d
    sccs = G.topsort' $ G.condensation depgraph
    -- Initialize arrangements map
    arrs = M.fromList $ map (, []) $ M.keys $ progRelations d
    -- Initialize types
    -- Make sure that empty tuple is always in Value, so it can be
    -- used to implement Value::default()
    types = S.fromList $ (tTuple []) : (map (typeNormalize d . relType) $ M.elems $ progRelations d)
    -- Compile SCCs
    (prog, cstate) = runState (do -- First pass: compute arrangements
                                  createArrangements d
                                  -- Second pass: compile relations
                                  nodes <- mapM (compileSCC d depgraph) sccs
                                  mkProg nodes)
                              $ emptyCompilerState { cArrangements = arrs
                                                   , cTypes        = types }
    -- Type declarations
    typedefs = vcat $ map (mkTypedef d) $ M.elems $ progTypedefs d
    -- Functions
    (fdef, fextern) = partition (isJust . funcDef) $ M.elems $ progFunctions d
    funcs = vcat $ (map (mkFunc d) fextern ++ map (mkFunc d) fdef) 

-- Add dummy relation to the spec if it does not contain any.
-- Otherwise, we have to tediously handle this corner case in various
-- parts of the compiler.
-- Likewise, add a dummy index if the spec does not contain any.
addDummyRel :: DatalogProgram -> DatalogProgram
addDummyRel d =
    d {progRelations = rels, progIndexes = idxs}
    where
    rels = if (M.null $ progRelations d) || (M.null $ progIndexes d)
              then M.insert "__Null" (Relation nopos RelInternal "__Null" (tTuple []) Nothing) (progRelations d)
              else progRelations d
    idxs = if M.null $ progIndexes d
              then M.singleton "__Null_by_none" $ Index nopos "__Null_by_none" [] $ Atom nopos "__Null" $ eTuple []
              else progIndexes d

mkTypedef :: DatalogProgram -> TypeDef -> Doc
mkTypedef d tdef@TypeDef{..} =
    vcat (map (\attr -> "#[" <> pp attr <> "]") rustAttrs) $$
    case tdefType of
         Just TStruct{..} | length typeCons == 1
                          -> derive_struct                                                             $$
                             "pub struct" <+> rname tdefName <> targs <+> "{"                          $$
                             (nest' $ vcat $ punctuate comma $ map (mkField True) $ consArgs $ head typeCons) $$
                             "}"                                                                       $$
                             impl_abomonate                                                            $$
                             mkFromRecord tdef                                                         $$
                             mkStructIntoRecord tdef                                                   $$
                             mkStructMutator tdef                                                      $$
                             display
                          | otherwise
                          -> derive_enum                                                               $$
                             "pub enum" <+> rname tdefName <> targs <+> "{"                            $$
                             (nest' $ vcat $ punctuate comma $ map mkConstructor typeCons)             $$
                             "}"                                                                       $$
                             impl_abomonate                                                            $$
                             mkFromRecord tdef                                                         $$
                             mkEnumIntoRecord tdef                                                     $$
                             mkEnumMutator tdef                                                        $$
                             display                                                                   $$
                             default_enum
         Just t           -> "pub type" <+> rname tdefName <+> targs <+> "=" <+> mkType t <> ";"
         Nothing          -> empty -- The user must provide definitions of opaque types
    where
    rustAttrs = getRustAttrs tdefAttrs
    derive_struct = "#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Default)]"
    derive_enum = "#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize)]"
    targs = if null tdefArgs
               then empty
               else "<" <> (hsep $ punctuate comma $ map pp tdefArgs) <> ">"
    targs_traits = if null tdefArgs
                      then empty
                      else "<" <> (hsep $ punctuate comma $ map ((<> ": Val") . pp) tdefArgs) <> ">"
    targs_disp = if null tdefArgs
                    then empty
                    else "<" <> (hsep $ punctuate comma $ map ((<> ": fmt::Debug") . pp) tdefArgs) <> ">"
    targs_def = if null tdefArgs
                   then empty
                   else "<" <> (hsep $ punctuate comma $ map ((<> ": Default") . pp) tdefArgs) <> ">"


    mkField :: Bool -> Field -> Doc
    mkField pub f = vcat (map (\attr -> "#[" <> pp attr <> "]") rattrs) $$
                    (if pub then "pub" else empty) <+> pp (name f) <> ":" <+> mkType f
        where rattrs = getRustAttrs $ fieldAttrs f

    mkConstructor :: Constructor -> Doc
    mkConstructor c =
        let args = vcat $ punctuate comma $ map (mkField False) $ consArgs c
            rattrs = getRustAttrs $ consAttrs c in
        vcat (map (\attr -> "#[" <> pp attr <> "]") rattrs) $$
        if null $ consArgs c
           then rname (name c)
           else rname (name c) <+> "{" $$
                nest' args $$
                "}"

    impl_abomonate = "impl" <+> targs_traits <+> "abomonation::Abomonation for" <+> rname tdefName <> targs <> "{}"

    display = "impl" <+> targs_disp <+> "fmt::Display for" <+> rname tdefName <> targs <+> "{"                 $$
              "    fn fmt(&self, __formatter: &mut fmt::Formatter) -> fmt::Result {"                           $$
              "        match self {"                                                                           $$
              (nest' $ nest' $ nest' $ vcat $ punctuate comma $ map mkDispCons $ typeCons $ fromJust tdefType) $$
              "        }"                                                                                      $$
              "    }"                                                                                          $$
              "}"                                                                                              $$
              "impl" <+> targs_disp <+> "fmt::Debug for" <+> rname tdefName <> targs <+> "{"                   $$
              "    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {"                                     $$
              "        fmt::Display::fmt(&self, f)"                                                            $$
              "    }"                                                                                          $$
              "}"
    mkDispCons :: Constructor -> Doc
    mkDispCons c@Constructor{..} =
        cname <> "{" <> (hcat $ punctuate comma $ map (pp . name) consArgs) <> "} => {" $$
        (nest' $
            "__formatter.write_str(\"" <> (pp $ name c) <> "{\")?;" $$
            (vcat $
             mapIdx (\a i -> (if isString d a
                                 then "record::format_ddlog_str(" <> (pp $ name a) <> ", __formatter)?;"
                                 else "fmt::Debug::fmt(" <> (pp $ name a) <> ", __formatter)?;")
                             $$
                             (if i + 1 < length consArgs
                                 then "__formatter.write_str(\",\")?;"
                                 else empty))
                    consArgs) $$
            "__formatter.write_str(\"}\")")
        $$
        "}"
        where cname = mkConstructorName tdefName (fromJust tdefType) (name c)

    default_enum =
              "impl "  <+> targs_def <+> "Default for" <+> rname tdefName <> targs <+> "{"                     $$
              "    fn default() -> Self {"                                                                     $$
              "        " <> cname <> "{" <> def_args <> "}"                                                    $$
              "    }"                                                                                          $$
              "}"
        where c = head $ typeCons $ fromJust tdefType
              cname = mkConstructorName tdefName (fromJust tdefType) (name c)
              def_args = commaSep $ map (\a -> (pp $ name a) <+> ": Default::default()") $ consArgs c
{-
Generate FromRecord trait implementation for a struct type:

impl <T: FromRecord> FromRecord for DummyEnum<T> {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Struct(constr, args) => {
                match constr.as_ref() {
                    "Constr1" if args.len() == 2 => {
                        Ok(DummyEnum::Constr1{f1: <Bbool>::from_record(&args[0])?,
                                              f2: String::from_record(&args[1])?})
                    },
                    "Constr2" if args.len() == 3 => {
                        Ok(DummyEnum::Constr2{f1: <T>::from_record(&args[0])?,
                                              f2: <BigInt>::from_record(&args[1])?,
                                              f3: <Foo<T>>::from_record(&args[2])?})
                    },
                    "Constr3" if args.len() == 1 => {
                        Ok(DummyEnum::Constr3{f1: <(bool,bool)>::from_record(&args[0])?})
                    },
                    c => Result::Err(format!("unknown constructor {} of type DummyEnum in {:?}", c, *val))
                }
            },
            v => {
                Result::Err(format!("not a struct {:?}", *v))
            }
        }
    }
}
-}
mkFromRecord :: TypeDef -> Doc
mkFromRecord t@TypeDef{..} =
    "impl" <+> targs_bounds <+> "record::FromRecord for" <+> rname (name t) <> targs <+> "{"                                    $$
    "    fn from_record(val: &record::Record) -> result::Result<Self, String> {"                                                $$
    "        match val {"                                                                                                       $$
    "            record::Record::PosStruct(constr, _args) => {"                                                                 $$
    "                match constr.as_ref() {"                                                                                   $$
    (nest' $ nest' $ nest' $ nest' $ nest' pos_constructors)                                                                    $$
    "                    c => result::Result::Err(format!(\"unknown constructor {} of type" <+> rname (name t) <+> "in {:?}\", c, *val))" $$
    "                }"                                                                                                         $$
    "            },"                                                                                                            $$
    "            record::Record::NamedStruct(constr, _args) => {"                                                               $$
    "                match constr.as_ref() {"                                                                                   $$
    (nest' $ nest' $ nest' $ nest' $ nest' named_constructors)                                                                  $$
    "                    c => result::Result::Err(format!(\"unknown constructor {} of type" <+> rname (name t) <+> "in {:?}\", c, *val))" $$
    "                }"                                                                                                         $$
    "            },"                                                                                                            $$
    "            v => {"                                                                                                        $$
    "                result::Result::Err(format!(\"not a struct {:?}\", *v))"                                                           $$
    "            }"                                                                                                             $$
    "        }"                                                                                                                 $$
    "    }"                                                                                                                     $$
    "}"
    where
    targs = "<" <> (hcat $ punctuate comma $ map pp tdefArgs) <> ">"
    targs_bounds = "<" <> (hcat $ punctuate comma $ map ((<> ": record::FromRecord + Default") . pp) tdefArgs) <> ">"
    pos_constructors = vcat $ map mkposcons $ typeCons $ fromJust tdefType
    mkposcons :: Constructor -> Doc
    mkposcons c@Constructor{..} =
        "\"" <> pp (name c) <> "\"" <+> "if _args.len() ==" <+> (pp $ length consArgs) <+> "=> {" $$
        "    Ok(" <> cname <> "{" <> (hsep $ punctuate comma fields) <> "})"     $$
        "},"
        where
        cname = mkConstructorName tdefName (fromJust tdefType) (name c)
        fields = mapIdx (\f i -> pp (name f) <> ": <" <> (mkType f) <> ">::from_record(&_args[" <> pp i <> "])?") consArgs
    named_constructors = vcat $ map mknamedcons $ typeCons $ fromJust tdefType
    mknamedcons :: Constructor -> Doc
    mknamedcons c@Constructor{..} =
        "\"" <> pp (name c) <> "\" => {" $$
        "    Ok(" <> cname <> "{" <> (hsep $ punctuate comma fields) <> "})"     $$
        "},"
        where
        cname = mkConstructorName tdefName (fromJust tdefType) (name c)
        fields = map (\f -> pp (name f) <> ": record::arg_extract::<" <> mkType f <> ">(_args, \"" <> (pp $ unddname f) <> "\")?") consArgs

mkStructIntoRecord :: TypeDef -> Doc
mkStructIntoRecord t@TypeDef{..} =
    "decl_struct_into_record!(" <> rname (name t) <> ", " <> targs <> "," <+> args <> ");"
    where
    targs = "<" <> (hcat $ punctuate comma $ map pp tdefArgs) <> ">"
    args = commaSep $ map (pp . name) $ consArgs $ head $ typeCons $ fromJust tdefType

mkStructMutator :: TypeDef -> Doc
mkStructMutator t@TypeDef{..} =
    "decl_record_mutator_struct!(" <> rname (name t) <> ", " <> targs <> "," <+> args <> ");"
    where
    targs = "<" <> (hcat $ punctuate comma $ map pp tdefArgs) <> ">"
    args = commaSep $ map (\arg -> pp (name arg) <> ":" <+> mkType arg) $ consArgs $ head $ typeCons $ fromJust tdefType

mkEnumIntoRecord :: TypeDef -> Doc
mkEnumIntoRecord t@TypeDef{..} =
    "decl_enum_into_record!(" <> rname (name t) <> "," <+> targs <> "," <+> cons <> ");"
    where
    targs = "<" <> (hcat $ punctuate comma $ map pp tdefArgs) <> ">"
    cons = commaSep $ map (\c -> (rname $ name c) <> "{" <> (commaSep $ map (pp . name) $ consArgs c) <> "}")
                    $ typeCons $ fromJust tdefType

mkEnumMutator :: TypeDef -> Doc
mkEnumMutator t@TypeDef{..} =
    "decl_record_mutator_enum!(" <> rname (name t) <> "," <+> targs <> "," <+> cons <> ");"
    where
    targs = "<" <> (hcat $ punctuate comma $ map pp tdefArgs) <> ">"
    cons = commaSep $ map (\c -> (rname $ name c) <> "{" <> (commaSep $ map (\arg -> pp (name arg) <> ":" <+> mkType arg) $ consArgs c) <> "}")
                    $ typeCons $ fromJust tdefType

unddname :: (WithName a) => a -> String
unddname x = if isPrefixOf "__" (name x) && elem short reservedNames
                then short
                else name x
    where short = drop 2 $ name x

{-
 pub fn relValFromRecord(rel: Relations, rec: &Record) -> Result<Value, String> {
    match rel {
        Relations::Rel1 => {
            Ok(Value::Rel1Constr(<Rel1Type>::from_record(rec)?))
        },
        ..
        _ => Err(...)
    }
}
-}
mkValueFromRecord :: (?cfg::CompilerConfig) => DatalogProgram -> Doc
mkValueFromRecord d@DatalogProgram{..} =
    mkRelationsTryFromStr d                                                                         $$
    mkIsOutputRels d                                                                                $$
    mkIsInputRels d                                                                                 $$
    mkRelationsTryFromRelId d                                                                       $$
    mkRelId2Name d                                                                                  $$
    mkRelId2NameC                                                                                   $$
    mkRelIdMap d                                                                                    $$
    mkRelIdMapC d                                                                                   $$
    mkInputRelIdMap d                                                                               $$
    mkOutputRelIdMap d                                                                              $$
    mkIndexesTryFromStr d                                                                           $$
    mkIndexesTryFromIdxId d                                                                         $$
    mkIdxId2Name d                                                                                  $$
    mkIdxId2NameC                                                                                   $$
    mkIdxIdMap d                                                                                    $$
    mkIdxIdMapC d                                                                                   $$
    "pub fn relval_from_record(rel: Relations, _rec: &record::Record) -> result::Result<DDValue, String> {" $$
    "    match rel {"                                                                               $$
    (nest' $ nest' $ vcommaSep entries)                                                             $$
    "    }"                                                                                         $$
    "}"                                                                                             $$
    "pub fn relkey_from_record(rel: Relations, _rec: &record::Record) -> result::Result<DDValue, String> {" $$
    "    match rel {"                                                                               $$
    (nest' $ nest' $ vcommaSep key_entries)                                                         $$
    "        _ => Err(format!(\"relation {:?} does not have a primary key\", rel))"                 $$
    "    }"                                                                                         $$
    "}"                                                                                             $$
    "pub fn idxkey_from_record(idx: Indexes, _rec: &record::Record) -> result::Result<DDValue, String> {"   $$
    "    match idx {"                                                                               $$
    (nest' $ nest' $ vcommaSep idx_entries)                                                         $$
    "    }"                                                                                         $$
    "}"
    where
    entries = map mkrelval $ M.elems progRelations
    mkrelval :: Relation ->  Doc
    mkrelval rel@Relation{..} =
        "Relations::" <> rname(name rel) <+> "=> {"                                                                     $$
        "    Ok(Value::" <> mkValConstructorName d t <> (parens $ "<" <> mkType t <> ">::from_record(_rec)?).into_ddvalue()")   $$
        "}"
        where t = typeNormalize d relType
    key_entries = map mkrelkey $ filter (isJust . relPrimaryKey) $ M.elems progRelations
    mkrelkey :: Relation ->  Doc
    mkrelkey rel@Relation{..} =
        "Relations::" <> rname(name rel) <+> "=> {"                                                                     $$
        "    Ok(Value::" <> mkValConstructorName d t <> (parens $ "<" <> mkType t <> ">::from_record(_rec)?).into_ddvalue()")   $$
        "}"
        where t = typeNormalize d $ fromJust $ relKeyType d rel
    idx_entries = map mkidxkey $ M.elems progIndexes
    mkidxkey :: Index ->  Doc
    mkidxkey idx@Index{..} =
        "Indexes::" <> rname (name idx) <+> "=> {"                                                                      $$
        "    Ok(Value::" <> mkValConstructorName d t <> (parens $ "<" <> mkType t <> ">::from_record(_rec)?).into_ddvalue()")   $$
        "}"
        where t = typeNormalize d $ idxKeyType idx

-- Convert string to `enum Relations`
mkRelationsTryFromStr :: DatalogProgram -> Doc
mkRelationsTryFromStr d =
    "impl TryFrom<&str> for Relations {"                                  $$
    "    type Error = ();"                                                $$
    "    fn try_from(rname: &str) -> result::Result<Self, Self::Error> {" $$
    "         match rname {"                                              $$
                  (nest' $ nest' $ vcat $ entries)                        $$
    "             _  => Err(())"                                          $$
    "         }"                                                          $$
    "    }"                                                               $$
    "}"
    where
    entries = map mkrel $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = "\"" <> pp (name rel) <> "\" => Ok(Relations::" <> rname (name rel) <> "),"

mkIsOutputRels :: DatalogProgram -> Doc
mkIsOutputRels d =
    "impl Relations {" $$
    "    pub fn is_output(&self) -> bool {"        $$
    "        match self {"                         $$
                 (nest' $ nest' $ vcat $ entries)  $$
    "            _  => false"                      $$
    "        }"                                    $$
    "    }"                                        $$
    "}"
    where
    entries = map mkrel $ filter ((== RelOutput) .relRole) $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = "Relations::" <> rname (name rel) <> " => true,"

mkIsInputRels :: DatalogProgram -> Doc
mkIsInputRels d =
    "impl Relations {" $$
    "    pub fn is_input(&self) -> bool {"         $$
    "        match self {"                         $$
                 (nest' $ nest' $ vcat $ entries)  $$
    "            _  => false"                      $$
    "        }"                                    $$
    "    }"                                        $$
    "}"
    where
    entries = map mkrel $ filter ((== RelInput) .relRole) $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = "Relations::" <> rname (name rel) <> " => true,"

-- Convert RelId to enum Relations
mkRelationsTryFromRelId :: DatalogProgram -> Doc
mkRelationsTryFromRelId d =
    "impl TryFrom<RelId> for Relations {"                                 $$
    "    type Error = ();"                                                $$
    "    fn try_from(rid: RelId) -> result::Result<Self, Self::Error> {"  $$
    "         match rid {"                                                $$
                  (nest' $ nest' $ vcat $ entries)                        $$
    "             _  => Err(())"                                          $$
    "         }"                                                          $$
    "    }"                                                               $$
    "}"
    where
    entries = map mkrel $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = pp (relIdentifier d rel) <+> "=> Ok(Relations::" <> rname (name rel) <> "),"

mkRelId2Name :: DatalogProgram -> Doc
mkRelId2Name d =
    "pub fn relid2name(rid: RelId) -> Option<&'static str> {" $$
    "   match rid {"                                          $$
    (nest' $ nest' $ vcat $ entries)                          $$
    "       _  => None"                                       $$
    "   }"                                                    $$
    "}"
    where
    entries = map mkrel $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = pp (relIdentifier d rel) <+> "=> Some(&\"" <> pp (name rel) <> "\"),"

mkRelId2NameC :: Doc
mkRelId2NameC =
    "pub fn relid2cname(rid: RelId) -> Option<&'static ffi::CStr> {"        $$
    "    RELIDMAPC.get(&rid).map(|c: &'static ffi::CString|c.as_ref())"                            $$
    "}"

mkRelIdMap :: DatalogProgram -> Doc
mkRelIdMap d =
    "lazy_static! {"                                                        $$
    "    pub static ref RELIDMAP: FnvHashMap<Relations, &'static str> = {"  $$
    "        let mut m = FnvHashMap::default();"                            $$
    (nest' $ nest' $ vcat entries)                                          $$
    "        m"                                                             $$
    "   };"                                                                 $$
    "}"
    where
    entries = map mkrel $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = "m.insert(Relations::" <> rname (name rel) <> ", \"" <> pp (name rel) <> "\");"

mkRelIdMapC :: DatalogProgram -> Doc
mkRelIdMapC d =
    "lazy_static! {"                                                            $$
    "    pub static ref RELIDMAPC: FnvHashMap<RelId, ffi::CString> = {"         $$
    "        let mut m = FnvHashMap::default();"                                $$
    (nest' $ nest' $ vcat entries)                                              $$
    "        m"                                                                 $$
    "   };"                                                                     $$
    "}"
    where
    entries = map mkrel $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = "m.insert(" <> pp (relIdentifier d rel) <>
        ", ffi::CString::new(\"" <> pp (name rel) <> "\").unwrap_or_else(|_|ffi::CString::new(r\"Cannot convert relation name to C string\").unwrap()));"

mkInputRelIdMap :: DatalogProgram -> Doc
mkInputRelIdMap d =
    "lazy_static! {"                                                                $$
    "    pub static ref INPUT_RELIDMAP: FnvHashMap<Relations, &'static str> = {"    $$
    "        let mut m = FnvHashMap::default();"                                    $$
    (nest' $ nest' $ vcat entries)                                                  $$
    "        m"                                                                     $$
    "    };"                                                                        $$
    "}"
    where
    entries = map mkrel $ filter ((== RelInput) . relRole) $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = "m.insert(Relations::" <> rname (name rel) <> ", \"" <> pp (name rel) <> "\");"

mkOutputRelIdMap :: DatalogProgram -> Doc
mkOutputRelIdMap d =
    "lazy_static! {"                                                                $$
    "    pub static ref OUTPUT_RELIDMAP: FnvHashMap<Relations, &'static str> = {"   $$
    "        let mut m = FnvHashMap::default();"                                    $$
    (nest' $ nest' $ vcat entries)                                                  $$
    "        m"                                                                     $$
    "    };"                                                                        $$
    "}"
    where
    entries = map mkrel $ filter ((== RelOutput) . relRole) $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = "m.insert(Relations::" <> rname (name rel) <> ", \"" <> pp (name rel) <> "\");"

-- Convert string to `enum Indexes`
mkIndexesTryFromStr :: DatalogProgram -> Doc
mkIndexesTryFromStr d =
    "impl TryFrom<&str> for Indexes {"                                    $$
    "    type Error = ();"                                                $$
    "    fn try_from(iname: &str) -> result::Result<Self, Self::Error> {" $$
    "         match iname {"                                              $$
                  (nest' $ nest' $ vcat $ entries)                        $$
    "             _  => Err(())"                                          $$
    "         }"                                                          $$
    "    }"                                                               $$
    "}"
    where
    entries = map mkidx $ M.elems $ progIndexes d
    mkidx :: Index -> Doc
    mkidx idx = "\"" <> pp (name idx) <> "\" => Ok(Indexes::" <> rname (name idx) <> "),"

mkIndexesIntoArrId :: DatalogProgram -> CompilerState -> Doc
mkIndexesIntoArrId d CompilerState{..} =
    "pub fn indexes2arrid(idx: Indexes) -> ArrId {"                  $$
    "    match idx {"                                                $$
    (nest' $ nest' $ vcat $ entries)                                 $$
    "    }"                                                          $$
    "}"
    where
    entries = map mkidx $ M.elems $ progIndexes d
    mkidx :: Index -> Doc
    mkidx idx = "Indexes::" <> rname (name idx) <+> "=> (" <+> pp (relIdentifier d $ idxRelation d idx)
                                                    <> "," <+> pp (findidx idx) <> "),"
    findidx :: Index -> Int
    findidx idx@Index{..} =
        fromJust
        $ findIndex (elem idx . arngUsedInIndexes)
        $ cArrangements M.! atomRelation idxAtom

-- Convert IdxId to `enum Indexes`
mkIndexesTryFromIdxId :: DatalogProgram -> Doc
mkIndexesTryFromIdxId d =
    "impl TryFrom<IdxId> for Indexes {"                                   $$
    "    type Error = ();"                                                $$
    "    fn try_from(iid: IdxId) -> result::Result<Self, Self::Error> {"  $$
    "         match iid {"                                                $$
                  (nest' $ nest' $ vcat $ entries)                        $$
    "             _  => Err(())"                                          $$
    "         }"                                                          $$
    "    }"                                                               $$
    "}"
    where
    entries = map mkidx $ M.elems $ progIndexes d
    mkidx :: Index -> Doc
    mkidx idx = pp (idxIdentifier d idx) <+> "=> Ok(Indexes::" <> rname (name idx) <> "),"

mkIdxId2Name :: DatalogProgram -> Doc
mkIdxId2Name d =
    "pub fn indexid2name(iid: IdxId) -> Option<&'static str> {" $$
    "   match iid {"                                            $$
    (nest' $ nest' $ vcat $ entries)                            $$
    "       _  => None"                                         $$
    "   }"                                                      $$
    "}"
    where
    entries = map mkidx $ M.elems $ progIndexes d
    mkidx :: Index -> Doc
    mkidx idx = pp (idxIdentifier d idx) <+> "=> Some(&\"" <> pp (name idx) <> "\"),"

mkIdxId2NameC :: Doc
mkIdxId2NameC =
    "pub fn indexid2cname(iid: IdxId) -> Option<&'static ffi::CStr> {"      $$
    "    IDXIDMAPC.get(&iid).map(|c: &'static ffi::CString|c.as_ref())"     $$
    "}"

mkIdxIdMap :: DatalogProgram -> Doc
mkIdxIdMap d =
    "lazy_static! {"                                                        $$
    "    pub static ref IDXIDMAP: FnvHashMap<Indexes, &'static str> = {"    $$
    "        let mut m = FnvHashMap::default();"                            $$
    (nest' $ nest' $ vcat entries)                                          $$
    "        m"                                                             $$
    "   };"                                                                 $$
    "}"
    where
    entries = map mkidx $ M.elems $ progIndexes d
    mkidx :: Index -> Doc
    mkidx idx = "m.insert(Indexes::" <> rname (name idx) <> ", \"" <> pp (name idx) <> "\");"

mkIdxIdMapC :: DatalogProgram -> Doc
mkIdxIdMapC d =
    "lazy_static! {"                                                            $$
    "    pub static ref IDXIDMAPC: FnvHashMap<IdxId, ffi::CString> = {"         $$
    "        let mut m = FnvHashMap::default();"                                $$
    (nest' $ nest' $ vcat entries)                                              $$
    "        m"                                                                 $$
    "   };"                                                                     $$
    "}"
    where
    entries = map mkidx $ M.elems $ progIndexes d
    mkidx :: Index -> Doc
    mkidx idx = "m.insert(" <> pp (idxIdentifier d idx) <>
        ", ffi::CString::new(\"" <> pp (name idx) <> "\").unwrap_or_else(|_|ffi::CString::new(r\"Cannot convert index name to C string\").unwrap()));"

mkFunc :: DatalogProgram -> Function -> Doc
mkFunc d f@Function{..} | isJust funcDef =
    "pub fn" <+> rname (name f) <> tvars <> (parens $ hsep $ punctuate comma $ map mkArg funcArgs) <+> "->" <+> mkType funcType      $$
    "{"                                                                                                                              $$
    (nest' $ mkExpr d (CtxFunc f) (fromJust funcDef) EVal)                                                                           $$
    "}"
                        | -- generate commented out prototypes of extern functions for user convenvience.
                          otherwise = "/* fn" <+> rname (name f) <> tvars <> (parens $ hsep $ punctuate comma $ map mkArg funcArgs) <+> "->" <+> mkType funcType <+> "*/"
    where
    mkArg :: FuncArg -> Doc
    mkArg a = pp (name a) <> ":" <+> "&" <> (if argMut a then "mut" else empty) <+> mkType a
    tvars = case funcTypeVars f of
                 []  -> empty
                 tvs -> "<" <> (hcat $ punctuate comma $ map ((<> ": Val") . pp) tvs) <> ">"

-- Generate Value type as an enum with one entry per type in types
mkValType :: (?cfg::CompilerConfig) => DatalogProgram -> S.Set Type -> Doc
mkValType d types =
    "pub mod Value" $$
    (braces' $
        "use super::*;"               $$
        (vcat $ map mkVal $ S.toList types))
    where
    mkVal :: Type -> Doc
    mkVal t =
        "#[derive(Default, Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]"            $$
        "pub struct" <+> tname <+> parens ("pub" <+> mkType t) <> ";"                                               $$
        "impl abomonation::Abomonation for" <+> tname <+> "{}"                                                      $$
        "impl fmt::Display for" <+> tname <+> "{"                                                                   $$
        "    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {"                                            $$
        (nest' $ nest' $ if isString d t
            then "record::format_ddlog_str(&self.0, f)"
            else "fmt::Debug::fmt(&self.0, f)")                                                                     $$
        "    }"                                                                                                     $$
        "}"                                                                                                         $$
        "impl record::IntoRecord for" <+> tname <+> "{"                                                             $$
        "    fn into_record(self) -> record::Record {"                                                              $$
        "        self.0.into_record()"                                                                              $$
        "    }"                                                                                                     $$
        "}"                                                                                                         $$
        "impl record::Mutator<" <> tname <> "> for record::Record {"                                                $$
        "    fn mutate(&self, v: &mut" <+> tname <+> ") -> result::Result<(), std::string::String> {"               $$
        "        self.mutate(&mut v.0)"                                                                             $$
        "    }"                                                                                                     $$
        "}"                                                                                                         $$
        "//#[typetag::serde]"                                                                                       $$
        "decl_ddval_convert!{" <> tname <> "}"
        where tname = mkValConstructorName d t

-- Precompute the set of arrangements used by the program.  This is done as a separate
-- compiler pass to maximize arrangement sharing: if a particular key is only used in
-- semijoin operators, then it is sufficient to create a cheaper 'Arrangement.Set' for it.
-- If it is also used in a join operator or an index, then we create an 'Arrangement.Map'
-- and share it between all relevant joins, indexes, and semijoins.
createArrangements :: DatalogProgram -> CompilerMonad ()
createArrangements d = do
    -- Iterate through all rules in the program; precompute the set of arrangements for each
    -- relation.
    mapM_ (createRelArrangements d) $ progRelations d
    -- Arrangements for all program indexes.
    mapM_ (createIndexArrangement d) $ progIndexes d

createIndexArrangement :: DatalogProgram -> Index -> CompilerMonad ()
createIndexArrangement d idx@Index{..} = do
    let marr = arrangeInput d idxAtom (CtxIndex idx)
               $ map (\f -> (eVar $ name f, error $ "createIndexArrangement " ++ show idx ++ ": " ++ name f ++ " context should not be used")) idxVars
    case marr of
         Nothing -> error $ "createIndexArrangement " ++ show idx ++ ": failed to compute index arrangement."
         Just arr -> addJoinArrangement (atomRelation idxAtom) arr (Just idx)

createRelArrangements :: DatalogProgram -> Relation -> CompilerMonad ()
createRelArrangements d rel = mapM_ (createRuleArrangements d) $ relRules d $ name rel

createRuleArrangements :: DatalogProgram -> Rule -> CompilerMonad ()
createRuleArrangements d rule | ruleIsMultiway rule =
    mapM_ (\delta -> mapM_ (createRuleArrangement d delta)
                           [1..(length (ruleRHS delta) - 1)])
          $ multiwayRuleDeltas d rule
                              | otherwise = do
    -- Arrange the first atom in the rule if needed
    let arr = ruleArrangeFstLiteral d rule
    when (isJust arr)
         $ addJoinArrangement (atomRelation $ rhsAtom $ head $ ruleRHS rule) (fromJust arr) Nothing
    mapM_ (createRuleArrangement d rule) [1..(length (ruleRHS rule) - 1)]

createRuleArrangement :: DatalogProgram -> Rule -> Int -> CompilerMonad ()
createRuleArrangement d rule idx = do
    let rhs = ruleRHS rule !! idx
    let ctx = CtxRuleRAtom rule idx
    let rel = getRelation d $ atomRelation $ rhsAtom rhs
    let (arr, _) = normalizeArrangement d ctx $ atomVal $ rhsAtom rhs
    -- If the literal does not introduce new variables, it's a semijoin
    let is_semi = null $ ruleRHSNewVars d rule idx
    case rhs of
         RHSLiteral{rhsPolarity=True}  | is_semi   -> addSemijoinArrangement (name rel) arr
                                       | otherwise -> addJoinArrangement (name rel) arr Nothing
         RHSLiteral{rhsPolarity=False} -> addAntijoinArrangement (name rel) arr
         _                             -> return ()

-- Generate Rust struct for ProgNode
compileSCC :: (?cfg::CompilerConfig) => DatalogProgram -> DepGraph -> [G.Node] -> CompilerMonad ProgNode
compileSCC d dep nodes | recursive = compileSCCNode d relnames
                       | otherwise = case depnode of
                                          DepNodeRel rel -> compileRelNode d rel
                                          DepNodeApply a -> return $ compileApplyNode d a
    where
    recursive = any (\(from, to) -> elem from nodes && elem to nodes) $ G.edges dep
    relnames = map ((\(DepNodeRel rel) -> rel) . fromJust . G.lab dep) nodes
    depnode = fromJust $ G.lab dep $ head nodes

compileRelNode :: (?cfg::CompilerConfig) => DatalogProgram -> String -> CompilerMonad ProgNode
compileRelNode d relname = do
    rel <- compileRelation d relname
    return $ RelNode rel

{-
Generates a call to transformer function.  Assumes the following calling convention illustrated by
the example below:

* Every input relation corresponds to an argument of type &Collection<>
* This argument must be followed by an extra argument of closure type that extracts a typed record
  from 'Value'.
* Input functions correspond to input argument of corresponding function type
* There is an additional input argument per output relation that wraps an instance of the
  corresponding record type into a value.
* The function returns a tuple of one or more computed collections

DDlog declaration:

extern transformer SCC(Edges:   relation['E],
                       from:    function(e: 'E): 'N,
                       to:      function(e: 'E): 'N)
    -> (SCCLabels: relation [('N, 'N)])

Corresponding Rust declaration:
pub fn graph_SCC<S,V,E,N,EF,LF>(edges: &Collection<S,V,Weight>, _edges: EF,
                                from: fn(&E) -> N,
                                to:   fn(&E) -> N,
                                _scclabels: LF) -> (Collection<S,V,Weight>)
where
     S: Scope,
     S::Timestamp: Lattice + Ord,
     V: Val,
     N: Val,
     E: Val,
     EF: Fn(V) -> E + 'static,
     LF: Fn((N,N)) -> V + 'static
-}
compileApplyNode :: (?cfg::CompilerConfig) => DatalogProgram -> Apply -> ProgNode
compileApplyNode d Apply{..} = ApplyNode $
    "{fn transformer() -> Box<dyn for<'a> Fn(&mut std::collections::BTreeMap<RelId, collection::Collection<scopes::Child<'a, worker::Worker<communication::Allocator>, TS>,Value,Weight>>)> {" $$
    "    Box::new(|collections| {"                                                                                                                             $$
    "        let (" <> commaSep outputs <> ") =" <+> rname applyTransformer <> (parens $ commaSep inputs) <> ";"                                               $$
    (nest' $ nest' $ vcat update_collections)                                                                                                                  $$
    "    })"                                                                                                                                                   $$
    "}; transformer}"
  where
    Transformer{..} = getTransformer d applyTransformer
    inputs = concatMap (\(i, ti) ->
                         if hotypeIsFunction (hofType ti)
                            then [rname i]
                            else ["collections.get(&(" <> relId i <> ")).unwrap()", extractValue d (relType $ getRelation d i)])
                       (zip applyInputs transInputs)
             ++
             map (\o -> parens $ "|v|" <> mkValue d "v" (relType $ getRelation d o) <> ".into_ddvalue()") applyOutputs
    outputs = map rname applyOutputs
    update_collections = map (\o -> "collections.insert(" <> relId o <> "," <+> rname o <> ");") applyOutputs

extractValue :: (?cfg::CompilerConfig) => DatalogProgram -> Type -> Doc
extractValue d t = parens $
        "|" <> vALUE_VAR <> ": DDValue| unsafe { Value::" <> mkValConstructorName d t' <> "::from_ddvalue(" <> vALUE_VAR <> ") }.0.clone()"
    where t' = typeNormalize d t

compileSCCNode :: (?cfg::CompilerConfig) => DatalogProgram -> [String] -> CompilerMonad ProgNode
compileSCCNode d relnames = do
    prels <- mapM (\rel -> do prel <- compileRelation d rel
                              -- Only `distinct` relation if its weights can grow
                              -- unboundedly inside fixed point computation.
                              return $ RecProgRel prel $ not $ relIsBounded d rel) relnames
    return $ SCCNode prels

{- Generate Rust representation of relation and associated rules.

//Example code generated by this function:
let ancestor = {
    let ancestorset = ancestorset.clone();
    Relation {
        name:         "ancestor".to_string(),
        input:        false,
        id:           2,
        rules:        vec![
            Rule{
                rel: 1,
                xforms: vec![]
            },
            Rule{
                rel: 2,
                xforms: vec![XForm::Join{
                    afun:        &(arrange_by_snd as ArrangeFunc<Value>),
                    arrangement: (1,0),
                    jfun:        &(jfun as JoinFunc<Value>)
                }]
            }],
        arrangements: vec![
            Arrangement{
                name: "arrange_by_ancestor".to_string(),
                afun: &(arrange_by_fst as ArrangeFunc<Value>)
            },
            Arrangement{
                name: "arrange_by_self".to_string(),
                afun: &(arrange_by_self as ArrangeFunc<Value>)
            }],
        change_cb:    Arc::new(move |v,pol| set_update("ancestor", &ancestorset, v, pol))
    }
};
-}
compileRelation :: (?cfg::CompilerConfig) => DatalogProgram -> String -> CompilerMonad ProgRel
compileRelation d rn = do
    let rel@Relation{..} = getRelation d rn
    -- collect all rules for this relation
    let (facts, rules) =
                partition (null . ruleRHS)
                $ relRules d rn
    rules' <- mapM (compileRule d) rules
    facts' <- mapM (compileFact d) facts
    key_func <- maybe (return "None")
                      (\k -> do lambda <- compileKey d rel k
                                return $ "Some(" <> lambda <> ")")
                relPrimaryKey

    let cb = if relRole == RelOutput
                then "change_cb:    Some(sync::Arc::new(sync::Mutex::new(__update_cb.clone())))"
                else "change_cb:    None"
    arrangements <- gets $ (M.! rn) . cArrangements
    compiled_arrangements <- mapM (mkArrangement d rel) arrangements
    let code =
            "Relation {"                                                                                        $$
            "    name:         \"" <> pp rn <> "\".to_string(),"                                                $$
            "    input:        " <> (if relRole == RelInput then "true" else "false") <> ","                    $$
            "    distinct:     " <> (if relRole == RelOutput && not (relIsDistinctByConstruction d rel)
                                        then "true"
                                        else "false") <> ","  $$
            "    key_func:     " <> key_func <> ","                                                             $$
            "    id:           " <> relId rn <> ","                                                             $$
            "    rules:        vec!["                                                                           $$
            (nest' $ nest' $ vcat (punctuate comma rules') <> "],")                                             $$
            "    arrangements: vec!["                                                                           $$
            (nest' $ nest' $ vcat (punctuate comma compiled_arrangements) <> "],")                              $$
            (nest' cb)                                                                                          $$
            "}"
    return ProgRel{ prelName = rn, prelCode = code, prelFacts = facts' }

compileKey :: (?cfg::CompilerConfig) => DatalogProgram -> Relation -> KeyExpr -> CompilerMonad Doc
compileKey d rel@Relation{..} KeyExpr{..} = do
    v <- mkValue' d (CtxKey rel) keyExpr
    return $
        "(|" <> kEY_VAR <> ": &DDValue| {"                                                                                                      $$
        "    let ref" <+> pp keyVar <+> "= unsafe { Value::" <> mkValConstructorName d relType <> "::from_ddvalue_ref(" <> kEY_VAR <> ") }.0;"  $$
        "    " <> v                                                                                                                             $$
        "})"

{- Generate Rust representation of a ground fact -}
compileFact :: (?cfg::CompilerConfig) => DatalogProgram -> Rule -> CompilerMonad Doc
compileFact d rl@Rule{..} = do
    let rel = atomRelation $ head ruleLHS
    v <- mkValue' d (CtxRuleL rl 0) $ atomVal $ head ruleLHS
    return $ "(" <> relId rel <> "," <+> v <> ") /*" <> pp rl <> "*/"


-- If the rule contains a join, antijoin, or aggregation operator then its first literal
-- will have to be arranged before applying the operator.  This function checks
-- if this arrangement can be stored as an 'Arranged' collection and, if so, returns
-- the normalized representation of the arrangement.  (This requires that two
-- additional conditions are met: (1) all operators between the first literal and
-- the join operator are filters and (2) the required arrangement allows normalized
-- representation (e.g., we cannot arrange by arbitrary arithmetic expressions at
-- the moment)).
ruleArrangeFstLiteral :: DatalogProgram -> Rule -> Maybe Expr
ruleArrangeFstLiteral d rl@Rule{..} | null ruleRHS = Nothing
                                    | otherwise = input_arrangement
  where
    -- RHSCondition's between fstatom and the next operator that is not an RHSCondition.
    conds = takeWhile (rhsIsCondition . (ruleRHS !!)) [1 .. length ruleRHS - 1]
    -- Index of the next operator
    rhs_idx = length conds + 1
    -- Input arrangement expected by the next operator (if any)
    arrange_input_by = if rhs_idx == length ruleRHS
                          then Nothing
                          else rhsInputArrangement d rl rhs_idx
    -- If we're at the start of the rule and need to arrange the input relation, generate
    -- arrangement pattern.
    input_arrangement = if all (rhsIsFilterCondition . (ruleRHS !!)) conds
                           then maybe Nothing (arrangeInput d (rhsAtom $ head ruleRHS) (CtxRuleRAtom rl 0) . fst) arrange_input_by
                           else Nothing

compileRule :: (?cfg::CompilerConfig) => DatalogProgram -> Rule -> CompilerMonad Doc
compileRule d rl | ruleIsMultiway rl = compileMultiwayRule d rl
                 | otherwise         = compileRule' d rl 0 True

compileMultiwayRule :: (?cfg::CompilerConfig) => DatalogProgram -> Rule -> CompilerMonad Doc
compileMultiwayRule d rl@Rule{..} = do
    deltas <- mapM (compileDeltaRule d) $ multiwayRuleDeltas d rl
    return $ "/*" <+> pp rl <+> "*/"                                                    $$
             "Rule::DeltaRule {"                                                        $$
             "    description:" <+> pp (show $ show rl) <> ".to_string(),"              $$
             "    deltas: vec!["                                                        $$
             (nest' $ nest' $ vcommaSep deltas)                                         $$
             "    ]"                                                                    $$
             "}"

-- Convert a '#[multiway]' rule to a list of delta rules.
multiwayRuleDeltas :: DatalogProgram -> Rule -> [Rule]
multiwayRuleDeltas d rl@Rule{..} =
    -- Generate a delta rule for each literal.
    map (\lit ->
          -- Label all literals before 'lit' as delayed
          -- Move lit to the front.
          -- Re-order remaining literals to maximize joins.
          -- Pull each filter as early as possible inside the rule.
          ruleMoveCondsUp d
            $ (\_rl -> ruleOptimizeJoins d _rl 1)
            $ (\_rl -> ruleMoveRHSLeft _rl lit 0)
            $ rl{ruleRHS = mapIdx (\rhs i -> if i < lit && rhsIsLiteral rhs
                                             then rhs{rhsDelayed = True} else rhs)
                                  ruleRHS})
        literals
    -- Indices of literals in the rule.
    where
    literals = map snd
               $ filter (rhsIsPositiveLiteral . fst)
               $ zip ruleRHS [0..]

compileDeltaRule :: (?cfg::CompilerConfig) => DatalogProgram -> Rule -> CompilerMonad Doc
compileDeltaRule d rl = do
    let atom = rhsAtom $ head $ ruleRHS rl
    -- Open and filter first literal.
    open <- openAtom d vALUE_VAR rl 0 atom "return None"
    let filters = mkFilters d rl 0
    let join_indices = tail $ findIndices rhsIsPositiveLiteral $ ruleRHS rl
    let next_join_idx = head join_indices
    ret <- mkVarsTupleValue d
           $ (rhsVarsAfter d rl (next_join_idx - 1)) `intersect` (rhsVarsAfter d rl next_join_idx)
    let fmfun = "&{fn __f(" <> vALUE_VAR <> ": DDValue) -> Option<DDValue>" $$
                (braces' $ open $$
                           vcat filters $$
                           "Some(" <> ret <> ")")                           $$
                "__f}"
    -- Generate a `DeltaOp::Join` for each remaining join operator.
    delta_ops <- mapM (mkDeltaJoinOp d rl) join_indices
    return $ "/* " <+> pp rl <+> "*/"  $$
             "DeltaRule {"                                              $$
             "   rel:" <+> relId (atomRelation atom) <> ","             $$
             "   fmfun:" <+> fmfun <> ","                               $$
             "   ops: vec![" <> vcommaSep delta_ops <> "]"              $$
             "}"

{- Generate Rust representation of a Datalog rule

// Example Rust code generated by this function
-}

-- Recursively scan the body of the rule; generate XFormCollection.
-- 'last_rhs_idx' is the last RHS operator already processed by 'compileRule' invocations
-- up the call stack.  '0' means we're starting to process the rule and need
-- to generate the top-level 'Rule data structure'
--
-- 'input_val' - true if the relation generated by the last operator is a Value, not
-- tuple.  This is the case when we've only encountered antijoins so far (since antijoins
-- do not rearrange the input relation).
compileRule' :: (?cfg::CompilerConfig) => DatalogProgram -> Rule -> Int -> Bool -> CompilerMonad Doc
compileRule' d rl@Rule{..} last_rhs_idx input_val = {-trace ("compileRule " ++ show rl ++ " / " ++ show last_rhs_idx) $-} do
    -- First relation in the body of the rule
    let fstatom = rhsAtom $ head ruleRHS
    -- RHSConditions between last_rhs_idx and the next operator that is not an RHSCondition.
    let conds = takeWhile (rhsIsCondition . (ruleRHS !!)) [last_rhs_idx + 1 .. length ruleRHS - 1]
    -- Index of the next operator
    let rhs_idx = last_rhs_idx + length conds + 1
    -- Next RHS operator to process
    let rhs = ruleRHS !! rhs_idx
    -- Input arrangement expected by the next operator (if any)
    let arrange_input_by = if rhs_idx == length ruleRHS
                              then Nothing
                              else rhsInputArrangement d rl rhs_idx
    -- Input arrangement expected by the next operator (if any)
    let input_arrangement = ruleArrangeFstLiteral d rl
    -- If we're at the start of the rule and need to arrange the input relation, generate
    -- arrangement pattern.
    input_arrangement_id <- if last_rhs_idx == 0 && isJust input_arrangement
                               then getJoinArrangement (atomRelation fstatom) $ fromJust input_arrangement
                               else return Nothing
    -- Open up input constructor; bring Datalog variables into scope
    open <- if input_val
               then openAtom  d ("&" <> vALUE_VAR) rl 0 (rhsAtom $ head ruleRHS) "return None"
               else openTuple d ("&" <> vALUE_VAR) $ rhsVarsAfter d rl last_rhs_idx
    -- Apply filters and assignments between last_rhs_idx and rhs_idx
    let filters = mkFilters d rl last_rhs_idx
    let prefix = open $+$ vcat filters
    -- Generate XFormCollection or XFormArrangement for the 'rhs' operator.
    let mkArrangedOperator conditions inpval =
            case rhs of
                 RHSLiteral{rhsPolarity=True}  -> mkJoin d conditions inpval rl rhs_idx
                 RHSLiteral{rhsPolarity=False} -> mkAntijoin d conditions inpval rl rhs_idx
                 RHSAggregate{}                -> mkAggregate d conditions inpval rl rhs_idx
                 _                             -> error $ "compileRule: operator " ++ show rhs ++ " does not expect arranged input"
    let mkCollectionOperator | rhs_idx == length ruleRHS
                             = mkHead d prefix rl
                             | otherwise =
            case rhs of
                 RHSFlatMap _ v e -> mkFlatMap d prefix rl rhs_idx v e
                 _ -> error "compileRule: operator requires arranged input"

    -- If: input to the operator is an arranged collection
    -- Then: create Rule::ArrangementRule, pass conditions as input to mkJoin/Antijoin
    -- Else:
    --     If: At the start of the rule:
    --     Then: Generate Rule::CollectionRule;
    --     If: Next operator requires arranged input
    --     Then: Generate Arrange operator;
    case input_arrangement_id of
       Just arid -> do
            xform <- mkArrangedOperator conds input_val
            return $ "/*" <+> pp rl <+> "*/"                                                    $$
                     "Rule::ArrangementRule {"                                                  $$
                     "    description:" <+> pp (show $ show rl) <> ".to_string(),"              $$
                     "    arr: (" <+> relId (atomRelation fstatom) <> "," <+> pp arid <> "),"   $$
                     "    xform:" <+> xform                                                     $$
                     "}"
       Nothing -> do
            xform <- case arrange_input_by of
                        Just (key_vars, val_vars) -> do
                            -- Evaluate arrange_input_by in the context of 'rhs'
                            let key_str = parens $ commaSep $ map (pp . fst) key_vars
                            akey <- mkTupleValue d key_vars
                            aval <- mkVarsTupleValue d val_vars
                            let afun = braces'
                                       $ prefix $$
                                         "Some((" <> akey <> "," <+> aval <> "))"
                            xform' <- mkArrangedOperator [] False
                            return $ "XFormCollection::Arrange {"                                                                                    $$
                                     "    description:" <+> (pp $ show $ show $ "arrange" <+> rulePPPrefix rl (last_rhs_idx+1) <+> "by" <+> key_str) <+> ".to_string(),"                                                                                                                             $$
                                     (nest' $ "afun: &{fn __f(" <> vALUE_VAR <> ": DDValue) -> Option<(DDValue,DDValue)>" $$ afun $$ "__f},")        $$
                                     "    next: Box::new(" <> xform' <> ")"                                                                          $$
                                     "}"
                        Nothing -> mkCollectionOperator
            return $
                if last_rhs_idx == 0
                   then "/*" <+> pp rl <+> "*/"                                         $$
                        "Rule::CollectionRule {"                                        $$
                        "    description:" <+> pp (show $ show rl) <> ".to_string(),"   $$
                        "    rel:" <+> relId (atomRelation fstatom) <> ","              $$
                        "    xform: Some(" <> xform <> ")"                              $$
                        "}"
                   else "Some(" <> xform <> ")"


-- 'Join', 'Antijoin', 'Semijoin', and 'Aggregate' operators take arranged collection
-- as input.  'rhsInputArrangement' returns the arrangement expected by the operator
-- or Nothing if the operator takes a flat collection (e.g., it's a 'FlatMap').
--
-- The first component of the return tuple is the list of expressions that must be used
-- to index the input collection.  The second component lists variables that
-- will form the value of the arrangement.
rhsInputArrangement :: DatalogProgram -> Rule -> Int -> Maybe ([(Expr, ECtx)], [Field])
rhsInputArrangement d rl rhs_idx =
    case ruleRHS rl !! rhs_idx of
        RHSLiteral{..} ->
            let ctx = CtxRuleRAtom rl rhs_idx
                (_, vmap) = normalizeArrangement d ctx $ atomVal rhsAtom
            in Just $ (map (\(_,e,c) -> (e,c)) vmap,
                       -- variables visible before join that are still in use after it
                       (rhsVarsAfter d rl (rhs_idx - 1)) `intersect` (rhsVarsAfter d rl rhs_idx))
        RHSAggregate{..} ->
            let ctx = CtxRuleRAggregate rl rhs_idx
            in Just $ (map (\v -> (eVar v, ctx)) rhsGroupBy,
                       -- all visible variables to preserve multiset semantics
                       rhsVarsAfter d rl (rhs_idx - 1))
        _ -> Nothing

mkFlatMap :: (?cfg::CompilerConfig) => DatalogProgram -> Doc -> Rule -> Int -> String -> Expr -> CompilerMonad Doc
mkFlatMap d prefix rl idx v e = do
    vars <- mkVarsTupleValue d $ rhsVarsAfter d rl idx
    -- Flatten
    let flatten = "let __flattened =" <+> mkExpr d (CtxRuleRFlatMap rl idx) e EVal <> ";"
    -- Clone variables before passing them to the closure.
    let clones = vcat $ map ((\vname -> "let" <+> vname <+> "=" <+> vname <> ".clone();") . pp . name)
                      $ filter ((/= v) . name) $ rhsVarsAfter d rl idx
    let fmfun = braces'
                $ prefix  $$
                  flatten $$
                  clones  $$
                  "Some(Box::new(__flattened.into_iter().map(move |" <> pp v <> "|" <> vars <> ")))"
    next <- compileRule' d rl idx False
    return $
        "XFormCollection::FlatMap{"                                                                                         $$
        "    description:" <+> (pp $ show $ show $ rulePPPrefix rl $ idx + 1) <+> ".to_string(),"                           $$
        (nest' $ "fmfun: &{fn __f(" <> vALUE_VAR <> ": DDValue) -> Option<Box<dyn Iterator<Item=DDValue>>>" $$ fmfun $$ "__f},")$$
        "    next: Box::new(" <> next <> ")"                                                                                $$
        "}"

mkAggregate :: (?cfg::CompilerConfig) => DatalogProgram -> [Int] -> Bool -> Rule -> Int -> CompilerMonad Doc
mkAggregate d filters input_val rl@Rule{..} idx = do
    let RHSAggregate{..} = ruleRHS !! idx
    let ctx = CtxRuleRAggregate rl idx
    let Just (_, group_vars) = rhsInputArrangement d rl idx
    -- Filter inputs before grouping
    ffun <- mkFFun d rl filters
    -- Function to extract the argument of aggregation function from 'Value'
    open <- if input_val
               then openAtom d vALUE_VAR rl 0 (rhsAtom $ head ruleRHS) "unreachable!()"
               else openTuple d vALUE_VAR group_vars
    let project = "&{fn __f(" <> vALUE_VAR <> ": &DDValue) -> " <+> mkType (exprType d ctx rhsAggExpr) $$
                  (braces' $ open $$ mkExpr d ctx rhsAggExpr EVal)                                   $$
                  "__f}"
    -- Aggregate function:
    -- - compute aggregate
    -- - return variables still in scope after this term
    let tmap = ruleAggregateTypeParams d rl idx
    let agg_func = getFunc d rhsAggFunc
    -- Pass group-by variables to the aggregate function.
    let grp = "&std_Group::new(&" <> (tupleStruct $ map (\v -> pp v <> ".clone()") rhsGroupBy) <> "," <+> gROUP_VAR <> "," <+> project <> ")"
    let tparams = commaSep $ map (\tvar -> mkType (tmap M.! tvar)) $ funcTypeVars agg_func
    let aggregate = "let" <+> pp rhsVar <+> "=" <+> rname rhsAggFunc <>
                    "::<" <> tparams <> ">(" <> grp <> ");"
    result <- mkVarsTupleValue d $ rhsVarsAfter d rl idx
    let key_vars = map (getVar d ctx) rhsGroupBy
    open_key <- openTuple d kEY_VAR key_vars
    let agfun = braces'
                $ open_key  $$
                  aggregate $$
                  result
    next <- compileRule' d rl idx False
    return $
        "XFormArrangement::Aggregate{"                                                                                           $$
        "    description:" <+> (pp $ show $ show $ rulePPPrefix rl $ idx + 1) <> ".to_string(),"                                 $$
        "    ffun:" <+> ffun <> ","                                                                                              $$
        "    aggfun: &{fn __f(" <> kEY_VAR <> ": &DDValue," <+> gROUP_VAR <> ": &[(&DDValue, Weight)]) -> DDValue" $$ agfun $$ "__f}," $$
        "    next: Box::new(" <> next <> ")"                                                                                     $$
        "}"

-- Generate Rust code to filter records and bring variables into scope.
-- The Rust code returns None if the record does not pass the filter.
--
-- let (v1,v2) /*v1,v2 are references*/ = match &v {
--     Value::Rel1(v1,v2) => (v1,v2),
--     _ => return None
-- };
openAtom :: (?cfg::CompilerConfig) => DatalogProgram -> Doc -> Rule -> Int -> Atom -> Doc -> CompilerMonad Doc
openAtom d var rl idx Atom{..} on_error = do
    let rel = getRelation d atomRelation
    let t = relType rel
    constructor <- mkValConstructorName' d t
    let varnames = map pp $ atomVars atomVal
        vars = tuple varnames
        mtch = mkMatch (mkPatExpr d (CtxRuleRAtom rl idx) atomVal EReference) vars on_error
    return $
        "let" <+> vars <+> "= match unsafe { " <+> constructor <> "::from_ddvalue_ref(" <> var <> ") }.0 {" $$
        (nest' mtch)                                                                                        $$
        "};"

-- Generate Rust code to open up tuples and bring variables into scope.
openTuple :: (?cfg::CompilerConfig) => DatalogProgram -> Doc -> [Field] -> CompilerMonad Doc
openTuple d var vs = do
    let t = tTuple $ map typ vs
    cons <- mkValConstructorName' d t
    let pattern = tupleStruct $ map (("ref" <+>) . pp . name) vs
    return $ "let" <+> pattern <+> "= unsafe {" <+> cons <> "::from_ddvalue_ref(" <+> var <+> ") }.0;"

-- Generate Rust constructor name for a type;
-- add type to CompilerMonad
mkValConstructorName' :: DatalogProgram -> Type -> CompilerMonad Doc
mkValConstructorName' d t = do
    let t' = typeNormalize d t
    addType t'
    return $ "Value::" <> mkValConstructorName d t'

-- Generate Rust constructor name for a type
mkValConstructorName :: DatalogProgram -> Type -> Doc
mkValConstructorName d t' =
    case t of
         TTuple{..}  -> "__Tuple" <> pp (length typeTupArgs) <> "__" <>
                        (hcat $ punctuate "_" $ map (mkValConstructorName d) typeTupArgs)
         TBool{}     -> "__Boolval"
         TInt{}      -> "__Intval"
         TString{}   -> "__Stringval"
         TBit{..}    -> "__Bitval" <> pp typeWidth
         TSigned{..} -> "__Signedval" <> pp typeWidth
         TUser{}     -> "__" <> consuser
         TOpaque{}   -> "__" <> consuser
         _           -> error $ "unexpected type " ++ show t ++ " in Compile.mkValConstructorName'"
    where
    t = typeNormalize d t'
    consuser = rname (typeName t) <>
               case typeArgs t of
                    [] -> empty
                    as -> "__" <> (hcat $ punctuate "_" $ map (mkValConstructorName d) as)

mkValue' :: (?cfg::CompilerConfig) => DatalogProgram -> ECtx -> Expr -> CompilerMonad Doc
mkValue' d ctx e = do
    let t = exprType d ctx e
    constructor <- mkValConstructorName' d t
    return $ constructor <> (parens $ mkExpr d ctx e EVal) <> ".into_ddvalue()"

mkValue :: (?cfg::CompilerConfig) => DatalogProgram -> Doc -> Type -> Doc
mkValue d v t = "Value::" <> mkValConstructorName d (typeNormalize d t) <> (parens v)

mkTupleValue :: (?cfg::CompilerConfig) => DatalogProgram -> [(Expr, ECtx)] -> CompilerMonad Doc
mkTupleValue d es = do
    let t = tTuple $ map (\(e, ctx) -> exprType'' d ctx e) es
    constructor <- mkValConstructorName' d t
    return $ constructor <> (parens $ tupleStruct $ map (\(e, ctx) -> mkExpr d ctx e EVal) es) <> ".into_ddvalue()"

mkVarsTupleValue :: (?cfg::CompilerConfig) => DatalogProgram -> [Field] -> CompilerMonad Doc
mkVarsTupleValue d vs = do
    let t = tTuple $ map typ vs
    constructor <- mkValConstructorName' d t
    return $ constructor <> (parens $ tupleStruct $ map ((<> ".clone()") . pp . name) vs) <> ".into_ddvalue()"

-- Compile all contiguous RHSCondition terms following 'last_idx'
mkFilters :: DatalogProgram -> Rule -> Int -> [Doc]
mkFilters d rl@Rule{..} last_idx =
    mapIdx (\rhs i -> mkFilter d (CtxRuleRCond rl $ i + last_idx + 1) $ rhsExpr rhs)
    $ takeWhile (\case
                  RHSCondition{} -> True
                  _              -> False)
    $ drop (last_idx+1) ruleRHS

-- Implement RHSCondition semantics in Rust; brings new variables into
-- scope if this is an assignment
mkFilter :: DatalogProgram -> ECtx -> Expr -> Doc
mkFilter d ctx (E e@ESet{..}) = mkAssignFilter d ctx e
mkFilter d ctx e              = mkCondFilter d ctx e

mkAssignFilter :: DatalogProgram -> ECtx -> ENode -> Doc
mkAssignFilter d ctx e@(ESet _ l r) =
    "let" <+> vardecls <+> "= match" <+> r' <+> "{"                 $$
    (nest' mtch)                                                    $$
    "};"
    where
    r' = mkExpr d (CtxSetR e ctx) r EVal
    mtch = mkMatch (mkPatExpr d (CtxSetL e ctx) l EVal) vars "return None"
    varnames = map (pp . fst) $ exprVarDecls (CtxSetL e ctx) l
    vars = tuple varnames
    vardecls = tuple $ map ("ref" <+>) varnames
mkAssignFilter _ _ e = error $ "Compile.mkAssignFilter: unexpected expression " ++ show e

mkCondFilter :: DatalogProgram -> ECtx -> Expr -> Doc
mkCondFilter d ctx e =
    "if !" <> mkExpr d ctx e EVal <+> "{return None;};"

-- Generate the ffun field of `XFormArrangement::Join/Semijoin/Aggregate`.
-- This field is used if input to the operator is an arranged relation.
mkFFun :: (?cfg::CompilerConfig) => DatalogProgram -> Rule -> [Int] -> CompilerMonad Doc
mkFFun _ Rule{} [] = return "None"
mkFFun d rl@Rule{..} input_filters = do
   open <- openAtom d vALUE_VAR rl 0 (rhsAtom $ ruleRHS !! 0) ("return false")
   let checks = hsep $ punctuate " &&"
                $ map (\i -> mkExpr d (CtxRuleRCond rl i) (rhsExpr $ ruleRHS !! i) EVal) input_filters
   return $ "Some(&{fn __f(" <> vALUE_VAR <> ": &DDValue) -> bool"   $$
            (braces' $ open $$ checks)                             $$
            "    __f"                                              $$
            "})"

-- Compile XForm::Join or XForm::Semijoin
-- Returns generated xform and index of the last RHS term consumed by
-- the XForm
mkJoin :: (?cfg::CompilerConfig) => DatalogProgram -> [Int] -> Bool -> Rule -> Int -> CompilerMonad Doc
mkJoin d input_filters input_val rl join_idx =
    _mkJoin d False input_filters input_val rl join_idx

mkDeltaJoinOp :: (?cfg::CompilerConfig) => DatalogProgram -> Rule -> Int -> CompilerMonad Doc
mkDeltaJoinOp d rl join_idx =
    _mkJoin d True [] False rl join_idx

_mkJoin :: (?cfg::CompilerConfig) => DatalogProgram -> Bool -> [Int] -> Bool -> Rule -> Int -> CompilerMonad Doc
_mkJoin d is_delta input_filters input_val rl@Rule{..} join_idx = do
    let rhs = ruleRHS !! join_idx
    let atom = rhsAtom rhs
    -- Build arrangement to join with
    let ctx = CtxRuleRAtom rl join_idx
    let (arr, _) = normalizeArrangement d ctx $ atomVal atom
    -- If the operator does not introduce new variables then it's a semijoin
    let semijoin = null $ ruleRHSNewVars d rl join_idx
    semi_arr_idx <- getSemijoinArrangement (atomRelation atom) arr
    join_arr_idx <- getJoinArrangement (atomRelation atom) arr
    -- Treat semijoin as normal join if we only have a map arrangement for it
    let (aid, is_semi) = if semijoin
                            then maybe (fromJust join_arr_idx, False) (,True) semi_arr_idx
                            else (fromJust join_arr_idx, False)
    -- Variables from previous terms visible in the join term.
    let pre_join_vars = rhsVarsAfter d rl (join_idx - 1)
    -- Variables from previous terms that will be used in terms
    -- following the join.
    let post_join_vars = pre_join_vars `intersect` (rhsVarsAfter d rl join_idx)
    -- Open input Value or tuple.  We may need to do this twice: once to filter
    -- input before join and once to filter output of the join.
    let open_input v = if input_val
                          then openAtom d v rl 0 (rhsAtom $ ruleRHS !! 0) "return None"
                          else openTuple d v $ if is_delta then pre_join_vars else post_join_vars
    -- Filter inputs using 'input_filters'
    ffun <- mkFFun d rl input_filters
    -- simplify pattern to only extract new variables from it
    let simplify (E e@EStruct{..})  = E e{exprStructFields = map (\(n,v) -> (n, simplify v)) exprStructFields}
        simplify (E e@ETuple{..})   = E e{exprTupleFields = map simplify exprTupleFields}
        simplify (E e@EVar{..}) | isNothing $ lookupVar d ctx exprVar
                                 = E e
        simplify (E e@EBinding{..}) = E e{exprPattern = simplify exprPattern}
        simplify (E e@ETyped{..})   = E e{exprExpr = simplify exprExpr}
        simplify (E e@ERef{..})     = E e{exprPattern = simplify exprPattern}
        simplify _                  = ePHolder
    -- Join function: open up both values, apply filters.
    open <- let (v1,v2) = if is_delta
                          then ("&" <> vALUE_VAR12 <> ".0", "&" <> vALUE_VAR12 <> ".1")
                          else (vALUE_VAR1, vALUE_VAR2) in
            if is_semi
               then open_input v1
               else liftM2 ($$) (open_input v1)
                           (openAtom d v2 rl join_idx (atom{atomVal = simplify $ atomVal atom}) "return None")
    -- DeltaOp additionally requires opening input to extract key from it.
    kopen <- open_input vALUE_VAR
    let filters = mkFilters d rl join_idx
        last_idx = join_idx + length filters
    -- If we're at the end of the rule, generate head atom; otherwise
    -- return all live variables in a tuple
    (ret, next) <- if last_idx == length ruleRHS - 1
        then (, "None") <$> mkValue' d (CtxRuleL rl 0) (atomVal $ head $ ruleLHS)
        else do ret <- mkVarsTupleValue d $ rhsVarsAfter d rl last_idx
                next <- compileRule' d rl last_idx False
                return (ret, next)
    let jfun = braces' $ open                     $$
                         vcat filters             $$
                         "Some" <> parens ret
    if is_delta
        then do
            let Just (key_vars, _) = rhsInputArrangement d rl join_idx
            kval <- mkVarsTupleValue d $ map (\(E (EVar _ v),ctx') -> Field nopos v $ exprType d ctx' $ eVar v) key_vars
            let kfun = "&{fn __f(" <> vALUE_VAR <> ": &DDValue) -> DDValue" $$
                       (braces' $ kopen $$ kval)  $$
                       "__f}"
            return $ if is_semi
            then "DeltaOp::Semijoin {"                                                                                           $$
                 "    keyfunc:" <+> kfun <> ","                                                                                  $$
                 "    arrangement: (" <> relId (atomRelation atom) <> "," <+> pp aid <> "),"                                     $$
                 "    timestamp:" <+> (if rhsDelayed rhs then "OldNew::Old" else "OldNew::New") <> ","                           $$
                 "    pfunc: &{fn __f(" <> vALUE_VAR12 <> ": (DDValue, ())) -> Option<DDValue>"                                  $$
                 nest' jfun                                                                                                      $$
                 "    __f},"                                                                                                     $$
                 "}"
            else "DeltaOp::Join {"                                                                                               $$
                 "    keyfunc:" <+> kfun <> ","                                                                                  $$
                 "    arrangement: (" <> relId (atomRelation atom) <> "," <+> pp aid <> "),"                                     $$
                 "    timestamp:" <+> (if rhsDelayed rhs then "OldNew::Old" else "OldNew::New") <> ","                           $$
                 "    pfunc: &{fn __f(" <> vALUE_VAR12 <> ": (DDValue, DDValue)) -> Option<DDValue>"                             $$
                 nest' jfun                                                                                                      $$
                 "    __f},"                                                                                                     $$
                 "}"
        else
            return $ if is_semi
            then "XFormArrangement::Semijoin{"                                                                                   $$
                 "    description:" <+> (pp $ show $ show $ rulePPPrefix rl $ join_idx + 1) <> ".to_string(),"                   $$
                 "    ffun:" <+> ffun <> ","                                                                                     $$
                 "    arrangement: (" <> relId (atomRelation atom) <> "," <> pp aid <> "),"                                      $$
                 "    jfun: &{fn __f(_: &DDValue ," <> vALUE_VAR1 <> ": &DDValue,_" <> vALUE_VAR2 <> ": &()) -> Option<DDValue>" $$
                 nest' jfun                                                                                                      $$
                 "    __f},"                                                                                                     $$
                 "    next: Box::new(" <> next  <> ")"                                                                           $$
                 "}"
            else "XFormArrangement::Join{"                                                                                       $$
                 "    description:" <+> (pp $ show $ show $ rulePPPrefix rl $ join_idx + 1) <> ".to_string(),"                   $$
                 "    ffun:" <+> ffun <> ","                                                                                     $$
                 "    arrangement: (" <> relId (atomRelation atom) <> "," <> pp aid <> "),"                                      $$
                 "    jfun: &{fn __f(_: &DDValue ," <> vALUE_VAR1 <> ": &DDValue," <> vALUE_VAR2 <> ": &DDValue) -> Option<DDValue>" $$
                 nest' jfun                                                                                                      $$
                 "    __f},"                                                                                                     $$
                 "    next: Box::new(" <> next <> ")"                                                                            $$
                 "}"

-- Compile XForm::Antijoin
mkAntijoin :: (?cfg::CompilerConfig) => DatalogProgram -> [Int] -> Bool -> Rule -> Int -> CompilerMonad Doc
mkAntijoin d input_filters input_val rl@Rule{..} ajoin_idx = do
    let Atom{..} = rhsAtom $ ruleRHS !! ajoin_idx
    -- create arrangement to anti-join with
    let ctx = CtxRuleRAtom rl ajoin_idx
    let (arr, _) = normalizeArrangement d ctx atomVal
    -- Filter inputs using 'input_filters'
    ffun <- mkFFun d rl input_filters
    aid <- fromJust <$> getAntijoinArrangement atomRelation arr
    next <- compileRule' d rl ajoin_idx input_val
    return $ "XFormArrangement::Antijoin {"                                                                   $$
             "    description:" <+> (pp $ show $ show $ rulePPPrefix rl $ ajoin_idx + 1) <> ".to_string(),"   $$
             "    ffun:" <+> ffun <> ","                                                                      $$
             "    arrangement: (" <> relId atomRelation <> "," <> pp aid <> "),"                              $$
             "    next: Box::new(" <> next <> ")"                                                             $$
             "}"

-- Normalized representation of an arrangement.  In general, an arrangement is characterized
-- by its key function 'key: Value->Option<Value>' that filters the input collection and computes
-- the value to index it by.  'key' can in principle be an arbitrary function, but we restrict
-- the set of supported functions to those that can be represented as a pattern, e.g.,
-- 'C1{_, C2{_0,_},_1}' represents a function that only matches records that satisfy the pattern
-- and returns a tuple '(_0,_1)'.  (Note that the ordering of auxiliary variables matters: the
-- above pattern is not equivalent to 'C1{_, C2{_1,_},_0}').
--
-- The following two functions are used to extract patterns from rules. Consider the following
-- rule:  '... :- A(x,z,B{_,y}), C(y.f1, D{z})'.  Given this rule we would like to generate
-- arrangements of 'C' and 'A'.
--
-- 'normalizeArrangement' computes the former: 'C{_0, D{_1}}', along with the mapping from aux
--    variables to expressions over original variables: '_0 -> y.f1, _1 -> z'.
--
-- 'arrangeInput' takes the mapping produced by 'normalizeArrangement' and computes the
--    latter: 'A{_,_1,B{_,Q{_0}}}'.  Note how the field expression 'y.f1' forced variable 'y'
--    to expand into a pattern 'Q{_0}'
--

-- Normalize pattern expression for use in arrangement:
-- * Eliminate parts of the pattern that do not constrain its value
-- * Replace subexpressions with variables.
normalizeArrangement :: DatalogProgram -> ECtx -> Expr -> (Expr, [(String, Expr, ECtx)])
normalizeArrangement d patctx pat = (renamed, vmap)
    where
    pat' = exprFoldCtx (normalizePattern d) patctx pat
    (renamed, (_, vmap)) = runState (rename patctx pat') (0, [])
    rename :: ECtx -> Expr -> State (Int, [(String, Expr, ECtx)]) Expr
    rename ctx (E e) =
        case e of
             EStruct{..}             -> do
                fs' <- mapM (\(n,e') -> (n,) <$> rename (CtxStruct e ctx n) e') exprStructFields
                return $ E e{exprStructFields = fs'}
             ETuple{..}              -> do
                fs' <- mapIdxM (\e' i -> rename (CtxTuple e ctx i) e') exprTupleFields
                return $ E e{exprTupleFields = fs'}
             EBool{}                 -> return $ E e
             EInt{}                  -> return $ E e
             EString{}               -> return $ E e
             EBit{}                  -> return $ E e
             ESigned{}               -> return $ E e
             EPHolder{}              -> return $ E e
             ETyped{..}              -> do
                e' <- rename (CtxTyped e ctx) exprExpr
                return $ E e{exprExpr = e'}
             ERef{..}                -> do
                e' <- rename (CtxRef e ctx) exprPattern
                return $ E e{exprPattern = e'}
             _ | exprIsConst d ctx (E e)   -> return $ E e
             _                       -> do
                vid <- gets fst
                let vname = "_" ++ show vid
                modify $ \(_, _vmap) -> (vid + 1, _vmap ++ [(vname, E e, ctx)])
                return $ eVar vname

-- Simplify away parts of the pattern that do not constrain its value.
normalizePattern :: DatalogProgram -> ECtx -> ENode -> Expr
normalizePattern d ctx e =
    case e of
         -- replace new variables with placeholders
         EVar{..}    | isNothing $ lookupVar d ctx exprVar
                                 -> ePHolder
         -- replace tuples and unique constructors populated with placeholders
         -- with a placeholder
         EStruct{..} | consIsUnique d exprConstructor && all ((== ePHolder) . snd) exprStructFields
                                 -> ePHolder
         ETuple{..}  | all (== ePHolder) exprTupleFields
                                 -> ePHolder
         EBinding{..}            -> exprPattern
         ERef{..}    | exprPattern == ePHolder
                                 -> ePHolder
         _                       -> E e

-- 'arrange_input_by' - list of expressions to arrange by that correspond to
-- '_0', '_1', ...   Returns normalized arrangement or 'Nothing' if
-- the arrangement cannot be represented in a normalized form
--
-- 'arrangeInput A(y, _, x) [x.f, y] = Just A(_1, _, C{.f=_0})'
-- 'arrangeInput A(y, _, x) [x.f+y] = Nothing' (since we currently can only arrange by
-- one or more fields and not any kind of derived expressions)
--
-- Step 1: replace each expression in 'arrange_input_by' by '_n' variables
-- Step 2: eliminate all other variables and patterns that do not constrain the value
arrangeInput :: DatalogProgram -> Atom -> ECtx -> [(Expr, ECtx)] -> Maybe Expr
arrangeInput d fstatom atomctx arrange_input_by = do
    fstatom' <- foldIdxM (subst atomctx) (atomVal fstatom) arrange_input_by
    let vars = map (\i -> "_" ++ show i) [0..length arrange_input_by - 1]
    return $ normalize vars fstatom'
  where
    subst :: ECtx -> Expr -> (Expr, ECtx) -> Int -> Maybe Expr
    subst ctx e arrange_by i =
        if exprIsVarOrField $ fst arrange_by
           then let (e', found) = runState (exprFoldCtxM (\ctx' x -> subst' ctx' x arrange_by i) ctx e) False
                -- 'found' can only be false here if 'fstatom' contains a binding 'x@expr',
                -- where both x and some field in 'expr' occur in 'arrange_input_by'.
                in if found then Just e' else Nothing
           else Nothing

    subst' :: ECtx -> ENode -> (Expr, ECtx) -> Int -> State Bool Expr
    subst' _   e@EBinding{..} ab i | exprVar == fieldExprVar (fst ab) = do {put True; return $ substVar ab i}
                                   | otherwise                        = return $ E e
    subst' _   e@EVar{..}     ab i | exprVar == fieldExprVar (fst ab) = do {put True; return $ substVar ab i}
                                   | otherwise                        = return $ E e
    -- We could just return 'e' unmodified in all other cases, but
    -- we enumerate them explicitly to catch unhandled cases.
    subst' _   e@ERef{}     _  _ = return $ E e
    subst' _   e@EStruct{}  _  _ = return $ E e
    subst' _   e@ETuple{}   _  _ = return $ E e
    subst' _   e@EBool{}    _  _ = return $ E e
    subst' _   e@EInt{}     _  _ = return $ E e
    subst' _   e@EString{}  _  _ = return $ E e
    subst' _   e@EBit{}     _  _ = return $ E e
    subst' _   e@ESigned{}  _  _ = return $ E e
    subst' _   e@EPHolder{} _  _ = return $ E e
    subst' _   e@ETyped{}   _  _ = return $ E e
    subst' ctx e            _  _ | exprIsConst d ctx (E e)
                                 = return $ E e
    subst' _   e            _  _ = error $ "Unexpected expression " ++ show e ++ " in Compile.arrangeInput.subst'"

    fieldExprVar (E EVar{..})   = exprVar
    fieldExprVar (E EField{..}) = fieldExprVar exprStruct
    fieldExprVar (E ETupField{..}) = fieldExprVar exprTuple
    fieldExprVar e              = error $ "Compile.arrangeInput.fieldExprVar " ++ show e

    substVar :: (Expr, ECtx) -> Int -> Expr
    substVar ab i = substVar' ab (eVar $ "_" ++ show i)

    substVar' :: (Expr, ECtx) -> Expr -> Expr
    substVar' (E EVar{}, _)          e' = e'
    substVar' (E par@(EField _ e f), ctx) e' = substVar' (e, ctx') e''
        where ctx' = CtxField par ctx
              TStruct _ [cons] = exprType' d ctx' e
              e'' = eStruct (name cons)
                    $ map (\a -> (name a, if name a == f then e' else ePHolder))
                    $ consArgs cons
    substVar' e                  _           = error $ "Unexpected expression " ++ show e ++ " in Compile.arrangeInput.substVar'"

    -- strip all variables but vs
    normalize :: [String] -> Expr -> Expr
    normalize vs e = exprFold (normalize' vs) e

    normalize' :: [String] -> ENode -> Expr
    normalize' vs e@EVar{..}   | elem exprVar vs                   = E e
                               | otherwise                         = ePHolder
    normalize' _  EStruct{..}  | consIsUnique d exprConstructor && all ((== ePHolder) . snd) exprStructFields
                               = ePHolder
    normalize' _  ETuple{..}   | all (== ePHolder) exprTupleFields = ePHolder
    normalize' _  EBinding{..} = exprPattern
    normalize' _  ERef{..}     | exprPattern == ePHolder           = ePHolder
    normalize' _  e                                                = E e

-- Compile XForm::FilterMap that generates the head of the rule
mkHead :: (?cfg::CompilerConfig) => DatalogProgram -> Doc -> Rule -> CompilerMonad Doc
mkHead d prefix rl = do
    v <- mkValue' d (CtxRuleL rl 0) (atomVal $ head $ ruleLHS rl)
    let fmfun = braces' $ prefix $$
                          "Some" <> parens v
    return $
        "XFormCollection::FilterMap{"                                                               $$
        "    description:" <+> (pp $ show $ show $ "head of" <+> pp rl) <+> ".to_string(),"         $$
        nest' ("fmfun: &{fn __f(" <> vALUE_VAR <> ": DDValue) -> Option<DDValue>" $$ fmfun $$ "__f},")  $$
        "    next: Box::new(None)"                                                                  $$
        "}"

-- Variables in the RHS of the rule declared before or in i'th term
-- and used after the term.
rhsVarsAfter :: DatalogProgram -> Rule -> Int -> [Field]
rhsVarsAfter d rl i =
    filter (\f -> -- If an aggregation occurs in the remaining part of the rule,
                  -- keep all variables to preserve multiset semantics
                  if any rhsIsAggregate $ drop (i+1) (ruleRHS rl)
                     then True
                     else elem (name f) $ (map name $ ruleLHSVars d rl) `union`
                                          (concatMap (ruleRHSTermVars d rl) [i+1..length (ruleRHS rl) - 1]))
           $ ruleRHSVars d rl (i+1)

mkProg :: [ProgNode] -> CompilerMonad Doc
mkProg nodes = do
    let rels = vcat $
               map (\ProgRel{..} -> "let" <+> rname prelName <+> "=" <+> prelCode <> ";")
                   (concatMap nodeRels nodes)
    let facts = concatMap prelFacts $ concatMap nodeRels nodes
    let pnodes = map mkNode nodes
        prog = "Program {"                                      $$
               "    nodes: vec!["                               $$
               (nest' $ nest' $ vcat $ punctuate comma pnodes)  $$
               "    ],"                                         $$
               "    init_data: vec!["                           $$
               (nest' $ nest' $ vcat $ punctuate comma facts)   $$
               "    ]"                                          $$
               "}"
    return $
        "pub fn prog(__update_cb: Box<dyn CBFn>) -> Program {"  $$
        (nest' $ rels $$ prog)                                                 $$
        "}"

mkNode :: ProgNode -> Doc
mkNode (RelNode (ProgRel rel _ _)) =
    "ProgNode::Rel{rel:" <+> rname rel <> "}"
mkNode (SCCNode rels) =
    "ProgNode::SCC{rels: vec![" <>
    (commaSep $ map (\RecProgRel{..} ->
                      "RecursiveRelation{rel: " <> (rname $ prelName rprelRel) <> 
                      ", distinct: " <> (if rprelDistinct then "true" else "false") <> "}") rels) <> "]}"
mkNode (ApplyNode fun) =
    "ProgNode::Apply{tfun:" <+> fun <> "}"

mkArrangement :: (?cfg::CompilerConfig) => DatalogProgram -> Relation -> Arrangement -> CompilerMonad Doc
mkArrangement d rel ArrangementMap{..} = do
    filter_key <- mkArrangementKey d rel arngPattern
    let afun = braces' $
               "let __cloned =" <+> vALUE_VAR <> ".clone();"                                                $$
               filter_key <> ".map(|x|(x,__cloned))"
    return $
        "Arrangement::Map{"                                                                                 $$
        "   name: r###\"" <> pp arngPattern <> "\"###.to_string(),"                                         $$
        (nest' $ "afun: &{fn __f(" <> vALUE_VAR <> ": DDValue) -> Option<(DDValue,DDValue)>" $$ afun $$ "__f},")  $$
        "    queryable:" <+> (if null arngIndexes then "false" else "true")                                 $$
        "}"

mkArrangement d rel ArrangementSet{..} = do
    filter_key <- mkArrangementKey d rel arngPattern
    let fmfun = braces' filter_key
    -- Arrangement contains distinct elements by construction and does
    -- not require expensive `distinct()` or `distinct_total()` applied to
    -- it if it is defined over all fields of a distinct relation (i.e.,
    -- the pattern expression does not contain placeholders).
    let distinct_by_construction = relIsDistinct d rel && (not $ exprContainsPHolders arngPattern)
    return $
        "Arrangement::Set{"                                                                            $$
        "    name: r###\"" <> pp arngPattern <> "\"###.to_string(),"                                   $$
        (nest' $ "fmfun: &{fn __f(" <> vALUE_VAR <> ": DDValue) -> Option<DDValue>" $$ fmfun $$ "__f},")   $$
        "    distinct:" <+> (if arngDistinct && not distinct_by_construction then "true" else "false") $$
        "}"

-- Generate part of the arrangement computation that filters inputs and computes the key part of the
-- arrangement.
mkArrangementKey :: (?cfg::CompilerConfig) => DatalogProgram -> Relation -> Expr -> CompilerMonad Doc
mkArrangementKey d rel pattern = do
    -- extract variables with types from pattern.
    let getvars :: Type -> Expr -> [Field]
        getvars t (E EStruct{..}) =
            concatMap (\(e,t') -> getvars t' e)
            $ zip (map snd exprStructFields) (map typ $ consArgs $ fromJust $ find ((== exprConstructor) . name) cs)
            where TStruct _ cs = typ' d t
        getvars t (E ETuple{..})  =
            concatMap (\(e,t') -> getvars t' e) $ zip exprTupleFields ts
            where TTuple _ ts = typ' d t
        getvars t (E ETyped{..})  = getvars t exprExpr
        getvars t (E ERef{..})    =
            getvars t' exprPattern
            where TOpaque _ _ [t'] = typ' d t
        getvars t (E EVar{..})    = [Field nopos [] exprVar t]
        getvars _ _               = []
    let t = relType rel
    -- Order variables by their integer value: '_0', '_1', ...
    patvars <- mkVarsTupleValue d
               $ sortBy (\f1 f2 -> compare ((read $ tail $ name f1)::Int) (read $ tail $ name f2))
               $ getvars t pattern
    constructor <- mkValConstructorName' d t
    let res = "Some(" <> patvars <> ")"
    let mtch = mkMatch (mkPatExpr d CtxTop pattern EReference) res "None"
    return $ "match" <+> "unsafe {" <+> constructor <> "::from_ddvalue(" <> vALUE_VAR <> ") }.0 {"  $$
             nest' mtch                                                                     $$
             "}"


-- Encodes Rust match pattern.
--
-- The first element is a Rust match pattern, the second
-- element is a (possibly empty) condition attached to the pattern,
-- e.g., the DDlog pattern
-- 'Constructor{f1= x, f2= "foo"}' compiles into
-- 'TypeName::Constructor{f1: x, f2=_0}' match and '*_0 == "foo".to_string())'
-- condition, where '_0' is an auxiliary variable of type 'String'.
--
-- The third field is only used if the input pattern contains 'ERef' expressions.
-- Rust does not allow pattern matching across smart pointers like 'Arc'; therefore
-- the content of an 'Arc' must be matched in a separare nested match expression.
-- (Note, this only works if there is a single pattern to match, which is why 'ERef'
-- is only allowed in rules.) Each tuple in the list (one per 'ERef' in the original
-- DDlog pattern) is an expression to match and the corresponding Rust pattern.
data Match = Match { mPattern::Doc, mCond::Doc, mSubpatterns::[(Doc, Match)] }

mkMatch :: Match -> Doc -> Doc -> Doc
mkMatch (Match pat cond subpatterns) if_matches if_misses =
    pat <+> "=>" <+> res <> "," $$
    "_ =>" <+> if_misses
    where
    res = if cond == empty
             then subpattern
             else "if" <+> cond <+> "{" $+$ nest' (nest' subpattern $+$ "} else {" <+> if_misses <+> "}")
    subpattern = case subpatterns of
        [] -> if_matches
        (e,p):subpatterns' ->
            "match" <+> e <+> "{"                                                                    $$
            (nest' $ mkMatch p{mSubpatterns = mSubpatterns p ++ subpatterns'} if_matches if_misses)  $$
            "}"

-- Compile Datalog pattern expression to Rust.
--
-- 'kind' - variables bound by this pattern must be of this kind.
--
-- Assumes:
-- * the expression being matched is of kind EVal
-- * if 'kind == EVal', the pattern does not contain any 'ERef's
mkPatExpr :: DatalogProgram -> ECtx -> Expr -> EKind -> Match
mkPatExpr d ctx (E e) kind = evalState (mkPatExpr' d EVal ctx e kind) 0

allocPatVar :: State Int Doc
allocPatVar = do
    i <- get
    put $ i+1
    return $ "_" <> pp i <> "_"

-- Computes lrefix to be attached to variables in pattern, given
-- the kind of relation being matched and the kind we want for the
-- new variable.
varprefix :: EKind -> EKind -> Doc
varprefix EReference EReference = empty
varprefix _          EReference = "ref"
varprefix EVal       EVal       = empty
varprefix inkind     varkind    = error $ "varprefix " ++ show inkind ++ " " ++ show varkind

mkPatExpr' :: DatalogProgram -> EKind -> ECtx -> ENode -> EKind -> State Int Match
mkPatExpr' _ inkind _   EVar{..}        varkind   = return $ Match (varprefix inkind varkind <+> pp exprVar) empty []
mkPatExpr' _ inkind _   EVarDecl{..}    varkind   = return $ Match (varprefix inkind varkind <+> pp exprVName) empty []
mkPatExpr' _ _      _   (EBool _ True)  _         = return $ Match "true" empty []
mkPatExpr' _ _      _   (EBool _ False) _         = return $ Match "false" empty []
mkPatExpr' _ inkind _   EString{..}     varkind   = do
    vname <- allocPatVar
    return $ Match (varprefix inkind varkind <+> vname) (vname <> ".as_str() ==" <+> "\"" <> pp exprString <> "\"") []
mkPatExpr' d inkind ctx e@EStruct{..}   varkind   = do
    fields <- mapM (\(f, E e') -> (f,) <$> mkPatExpr' d inkind (CtxStruct e ctx f) e' varkind) exprStructFields
    let t = consType d exprConstructor
        struct_name = name t
        pat = mkConstructorName struct_name (fromJust $ tdefType t) exprConstructor <>
              (braces $ hsep $ punctuate comma $ map (\(fname, m) -> pp fname <> ":" <+> mPattern m) fields)
        cond = hsep $ intersperse "&&" $ filter (/= empty)
                                       $ map (\(_,m) -> mCond m) fields
        subpatterns = concatMap (\(_, m) -> mSubpatterns m) fields
    return $ Match pat cond subpatterns
mkPatExpr' d inkind ctx e@ETuple{..}    varkind   = do
    fields <- mapIdxM (\(E f) i -> mkPatExpr' d inkind (CtxTuple e ctx i) f varkind) exprTupleFields
    let pat = tupleStruct $ map (pp . mPattern) fields
        cond = hsep $ intersperse "&&" $ filter (/= empty)
                                       $ map (pp . mCond) fields
        subpatterns = concatMap mSubpatterns fields
    return $ Match pat cond subpatterns
mkPatExpr' _ _      _   EPHolder{}      _         = return $ Match "_" empty []
mkPatExpr' d inkind ctx e@ETyped{..}    varkind   = mkPatExpr' d inkind (CtxTyped e ctx) (enode exprExpr) varkind
mkPatExpr' d inkind ctx e@EBinding{..}  varkind   = do
    -- Rust does not allow variable declarations inside bindings.
    -- To bypass this, we convert the bind pattern into a nested pattern.
    -- Unfortunately, this means that binding can only be used when there
    -- is a single pattern to match against, e.g., in an atom or an assignment
    -- clause in a rule.
    Match pat cond subpatterns <- mkPatExpr' d inkind (CtxBinding e ctx) (enode exprPattern) varkind
    return $ Match (varprefix inkind varkind <+> pp exprVar) empty ((pp exprVar, Match pat cond []):subpatterns)
mkPatExpr' d inkind ctx e@ERef{..}      varkind   = do
    vname <- allocPatVar
    subpattern <- mkPatExpr' d EReference {- deref() returns reference -}
                             (CtxRef e ctx) (enode exprPattern) varkind
    return $ Match (varprefix inkind varkind <+> vname)
                   empty [("(" <> deref (pp vname, varkind, undefined) <> ").deref()" , subpattern)]
mkPatExpr' d inkind ctx e               varkind   = do
    vname <- allocPatVar
    return $ Match (varprefix inkind varkind <+> vname)
                   (deref (vname, varkind, undefined) <+> "==" <+> mkExpr d ctx (E e) EVal)
                   []

-- Convert Datalog expression to Rust.
-- We generate the code so that all variables are references and must
-- be dereferenced before use or cloned when passed to a constructor,
-- assigned to another variable or returned.
mkExpr :: DatalogProgram -> ECtx -> Expr -> EKind -> Doc
mkExpr d ctx e k =
    case k of
         EVal       -> val e'
         EReference -> ref e'
         ELVal      -> lval e'
         ENoReturn  -> sel1 e'
    where
    e' = exprFoldCtx (mkExpr_ d) ctx e

mkExpr_ :: DatalogProgram -> ECtx -> ExprNode (Doc, EKind, ENode) -> (Doc, EKind, ENode)
mkExpr_ d ctx e = (t', k', e')
    where (t', k') = mkExpr' d ctx e
          e' = exprMap (E . sel3) e

-- Compiled expressions are represented as '(Doc, EKind)' tuple, where
-- the second components is the kind of the compiled representation
mkExpr' :: DatalogProgram -> ECtx -> ExprNode (Doc, EKind, ENode) -> (Doc, EKind)
-- All variables are references
mkExpr' _ _ EVar{..}    = (pp exprVar, EReference)

-- Function arguments are passed as read-only references
-- Functions return real values.
mkExpr' d _ EApply{..}  =
    (rname exprFunc <> (parens $ commaSep
                        $ map (\(a, mut) -> if mut then mutref a else ref a)
                        $ zip exprArgs (map argMut $ funcArgs f)), kind)
    where
    f = getFunc d exprFunc
    kind = if elem exprFunc fUNCS_RETURN_REF then EReference else EVal

-- Field access automatically dereferences subexpression
mkExpr' _ _ EField{..} = (sel1 exprStruct <> "." <> pp exprField, ELVal)
mkExpr' _ _ ETupField{..} = ("(" <> sel1 exprTuple <> "." <> pp exprTupField <> ")", ELVal)

mkExpr' _ _ (EBool _ True) = ("true", EVal)
mkExpr' _ _ (EBool _ False) = ("false", EVal)
mkExpr' _ _ EInt{..} = (mkInt exprIVal, EVal)
mkExpr' _ _ EString{..} = ("String::from(r###\"" <> pp exprString <> "\"###)", EVal)
mkExpr' _ _ EBit{..} | exprWidth <= 128 = (parens $ pp exprIVal <+> "as" <+> mkType (tBit exprWidth), EVal)
                     | otherwise        = ("Uint::parse_bytes(b\"" <> pp exprIVal <> "\", 10)", EVal)
mkExpr' _ _ ESigned{..} | exprWidth <= 128 = (parens $ pp exprIVal <+> "as" <+> mkType (tSigned exprWidth), EVal)
                        | otherwise        = ("Int::parse_bytes(b\"" <> pp exprIVal <> "\", 10)", EVal)

-- Struct fields must be values
mkExpr' d ctx EStruct{..} | ctxInSetL ctx
                          = (tname <> fieldlvals, ELVal)
                          | isstruct
                          = (tname <> fieldvals, EVal)
                          | otherwise
                          = (tname <> "::" <> rname exprConstructor <> fieldvals, EVal)
    where fieldvals  = braces $ commaSep $ map (\(fname, v) -> pp fname <> ":" <+> val v) exprStructFields
          fieldlvals = braces $ commaSep $ map (\(fname, v) -> pp fname <> ":" <+> lval v) exprStructFields
          tdef = consType d exprConstructor
          isstruct = isStructType $ fromJust $ tdefType tdef
          tname = rname $ name tdef

-- Tuple fields must be values
mkExpr' _ ctx ETuple{..} | ctxInSetL ctx
                         = (tupleStruct $ map lval exprTupleFields, ELVal)
                         | otherwise
                         = (tupleStruct $ map val exprTupleFields, EVal)

mkExpr' d ctx e@ESlice{..} = (mkSlice (val exprOp, w) exprH exprL, EVal)
    where
    e' = exprMap (E . sel3) e
    TBit _ w = exprType' d (CtxSlice e' ctx) $ E $ sel3 exprOp

-- Match expression is a reference
mkExpr' d ctx e@EMatch{..} = (doc, EVal)
    where
    e' = exprMap (E . sel3) e
    m = {-if exprIsVarOrFieldLVal d (CtxMatchExpr (exprMap (E . sel3) e) ctx) (E $ sel3 exprMatchExpr)
           then mutref exprMatchExpr
           else ref exprMatchExpr -}
        deref exprMatchExpr
    doc = ("match" <+> m <+> "{")
          $$
          (nest' $ vcat $ punctuate comma cases)
          $$
          "}"
    cases = mapIdx (\(c,v) idx -> let Match pat cond [] = mkPatExpr d (CtxMatchPat e' ctx idx) (E $ sel3 c) EReference
                                      cond' = if cond == empty then empty else ("if" <+> cond) in
                                  pat <+> cond' <+> "=>" <+> val v) exprCases

-- Variables are mutable references
mkExpr' _ _ EVarDecl{..} = ("ref mut" <+> pp exprVName, ELVal)

mkExpr' _ ctx ESeq{..} | ctxIsSeq2 ctx || ctxIsFunc ctx
                       = (body, EVal)
                       | otherwise
                       = (braces' body, EVal)
    where
    body = (sel1 exprLeft <> ";") $$ val exprRight

mkExpr' _ _ EITE{..} = (doc, EVal)
    where
    doc = ("if" <+> deref exprCond <+> "{") $$
          (nest' $ val exprThen)            $$
          ("}" <+> "else" <+> "{")          $$
          (nest' $ val exprElse)            $$
          "}"

mkExpr' d ctx e@EFor{..} = (doc, EVal)
    where
    e' = exprMap (E . sel3) e
    -- Iterators over groups and maps produces owned values, not references
    opt_ref = if (\t -> isGroup d t || isMap d t) $ exprType d (CtxForIter e' ctx) (E $ sel3 exprIter)
                 then "ref"
                 else empty
    doc = ("for" <+> opt_ref <+> pp exprLoopVar <+> "in" <+> sel1 exprIter <> ".iter() {") $$
          (nest' $ val exprBody)                                                   $$
          "}"

-- Desonctruction expressions in LHS are compiled into let statements, other assignments
-- are compiled into normal assignments.  Note: assignments in rule
-- atoms are handled by a different code path.
mkExpr' d ctx ESet{..} | islet     = ("let" <+> assign <> optsemi, EVal)
                       | otherwise = (assign, EVal)
    where
    islet = exprIsDeconstruct d $ E $ sel3 exprLVal
    assign = lval exprLVal <+> "=" <+> val exprRVal
    optsemi = if not (ctxIsSeq1 ctx) then ";" else empty

mkExpr' _ _ EBreak{} = ("break", ENoReturn)
mkExpr' _ _ EContinue{} = ("continue", ENoReturn)
mkExpr' _ _ EReturn{..} = ("return" <+> val exprRetVal, ENoReturn)

-- operators take values or lvalues and return values
mkExpr' d ctx e@EBinOp{..} = (v', EVal)
    where
    e1 = val exprLeft
    e2 = val exprRight
    e' = exprMap (E . sel3) e
    t  = exprType' d ctx (E e')
    t1 = exprType' d (CtxBinOpL e' ctx) (E $ sel3 exprLeft)
    t2 = exprType' d (CtxBinOpR e' ctx) (E $ sel3 exprRight)
    v = case exprBOp of
             Concat | t == tString
                    -> case sel3 exprRight of
                            EString _ s -> "string_append_str(" <> e1 <> ", r###\"" <> pp s <> "\"###)"
                            _           -> "string_append(" <> e1 <> "," <+> ref exprRight <> ")"
             op     -> mkBinOp d op (e1, t1) (e2, t2)

    -- Truncate bitvector result in case the type used to represent it
    -- in Rust is larger than the bitvector width.
    v' = if elem exprBOp bopsRequireTruncation
            then mkTruncate v t
            else v

mkExpr' d ctx e@EUnOp{..} = (v, EVal)
    where
    arg =  val exprOp
    e' = exprMap (E . sel3) e
    t = exprType' d ctx (E e')
    uint = (not $ isInt d t) && (typeWidth (typ' d t) <= 128)
    v = case exprUOp of
             Not    -> parens $ "!" <> arg
             BNeg   -> mkTruncate (parens $ "!" <> arg) t
             UMinus | uint
                    -> mkTruncate (parens $ arg <> ".wrapping_neg()") t
             UMinus -> mkTruncate (parens $ "-" <> arg) t
mkExpr' _ _ EPHolder{} = ("_", ELVal)

-- * Use type ascriptions in LHS of assignment
-- * Do type coercion for integer constants
-- * Otherwise, introduce an intermediate variable with explicit type
mkExpr' _ ctx ETyped{..} | ctxIsSetL ctx = (e' <+> ":" <+> mkType exprTSpec, categ)
                         | isint         = (parens $ e' <+> "as" <+> mkType exprTSpec, categ)
                         | otherwise     = (braces $ "let __typed:" <+> opt_ref <> mkType exprTSpec <+> "=" <+> e' <> "; __typed", categ)
    where
    (e', categ, e) = exprExpr
    opt_ref = case categ of
                   EReference -> "&"
                   _ -> empty
    isint = case e of
                 EInt{} -> True
                 _      -> False

mkExpr' d ctx EAs{..} | narrow_from && narrow_to && width_cmp /= GT
                      -- use Rust's type cast syntax to convert between
                      -- primitive types; no need to truncate the result if
                      -- target width is greater than or equal to source
                      = (parens $ val exprExpr <+> "as" <+> mkType exprTSpec, EVal)
                      | narrow_from && narrow_to
                      -- apply lossy type conversion between primitive Rust types;
                      -- truncate the result if needed
                      = (mkTruncate (parens $ val exprExpr <+> "as" <+> mkType exprTSpec) to_type,
                         EVal)
                      | width_cmp == GT && tfrom == tto
                      -- from_type is wider than to_type, but they both
                      -- correspond to the same Rust type: truncate from_type
                      -- (e & ((1 << w) - 1))
                      = ("(" <> val exprExpr <+>
                         "& ((" <> tfrom <> "::one() <<" <> pp (typeWidth to_type) <> ") -" <+> tfrom <> "::one()))"
                        , EVal)
                      | width_cmp == GT
                      -- from_type is wider than to_type: truncate from_type and
                      -- then convert:
                      -- (e & ((1 << w) - 1)).to_<to_type>().unwrap()
                      = ("(" <> val exprExpr <+>
                         "& ((" <> tfrom <> "::one() <<" <> pp (typeWidth to_type) <> ") -" <+> tfrom <> "::one()))" <>
                         ".to_" <> tto <> "().unwrap()", EVal)
                      | tto == tfrom
                      -- from_type is same width or narrower than to_type and
                      -- they both correspond to the same Rust type
                      = (val exprExpr, EVal)
                      | otherwise
                      = (parens $ tto <> "::from_" <> tfrom <> "(" <> val exprExpr <> ")", EVal)
    where
    e' = sel3 exprExpr
    from_type = exprType' d (CtxAs e' ctx) $ E e'
    to_type   = typ' d exprTSpec
    tfrom = mkType from_type
    tto   = mkType to_type
    narrow_from = (isBit d from_type || isSigned d from_type) && typeWidth from_type <= 128
    narrow_to   = (isBit d to_type || isSigned d to_type)  && typeWidth to_type <= 128
    width_cmp = if ((isBit d from_type || isSigned d from_type) &&
                    (isBit d to_type || isSigned d to_type))
                   then compare (typeWidth from_type) (typeWidth to_type)
                   else if isInt d from_type && isInt d to_type
                           then EQ
                           else if isInt d to_type then LT else GT

mkExpr' _ _ e = error $ "Compile.mkExpr': unexpected expression at " ++ show (pos e)

mkType :: (WithType a) => a -> Doc
mkType x = mkType' $ typ x

mkType' :: Type -> Doc
mkType' TBool{}                    = "bool"
mkType' TInt{}                     = "Int"
mkType' TString{}                  = "String"
mkType' TBit{..} | typeWidth <= 8  = "u8"
                 | typeWidth <= 16 = "u16"
                 | typeWidth <= 32 = "u32"
                 | typeWidth <= 64 = "u64"
                 | typeWidth <= 128= "u128"
                 | otherwise       = "Uint"
mkType' t@TSigned{..} | typeWidth == 8  = "i8"
                      | typeWidth == 16 = "i16"
                      | typeWidth == 32 = "i32"
                      | typeWidth == 64 = "i64"
                      | typeWidth == 128= "i128"
                      | otherwise       = errorWithoutStackTrace $ "Only machine widths (8/16/32/64/128) supported: " ++ show t
mkType' TTuple{..} | length typeTupArgs <= 12
                                   = parens $ commaSep $ map mkType' typeTupArgs
mkType' TTuple{..}                 = tupleTypeName typeTupArgs <>
                                     if null typeTupArgs
                                        then empty
                                        else "<" <> (commaSep $ map mkType' typeTupArgs) <> ">"
mkType' TUser{..}                  = rname typeName <>
                                     if null typeArgs
                                        then empty
                                        else "<" <> (commaSep $ map mkType' typeArgs) <> ">"
mkType' TOpaque{..}                = rname typeName <>
                                     if null typeArgs
                                        then empty
                                        else "<" <> (commaSep $ map mkType' typeArgs) <> ">"
mkType' TVar{..}                   = pp tvarName
mkType' t                          = error $ "Compile.mkType' " ++ show t

mkBinOp :: DatalogProgram -> BOp -> (Doc, Type) -> (Doc, Type) -> Doc
mkBinOp d op (e1, t1) (e2, t2) =
    case op of
        Eq     -> parens $ e1 <+> "==" <+> e2
        Neq    -> parens $ e1 <+> "!=" <+> e2
        Lt     -> parens $ e1 <+> "<"  <+> e2
        Gt     -> parens $ e1 <+> ">"  <+> e2
        Lte    -> parens $ e1 <+> "<=" <+> e2
        Gte    -> parens $ e1 <+> ">=" <+> e2
        And    -> parens $ e1 <+> "&&" <+> e2
        Or     -> parens $ e1 <+> "||" <+> e2
        Impl   -> parens $ "!" <> e1 <+> "||" <+> e2
        Mod    | uint
               -> parens $ e1 <> ".wrapping_rem(" <> e2 <> ")"
               | otherwise
               -> parens $ e1 <+> "%" <+> e2
        Div    | uint
               -> parens $ e1 <> ".wrapping_div(" <> e2 <> ")"
               | otherwise
               -> parens $ e1 <+> "/" <+> e2
        ShiftR | uint
               -> parens $ e1 <> ".wrapping_shr(" <> e2 <> ")"
               | otherwise
               -> parens $ e1 <+> ">>" <+> e2
        ShiftL | uint
               -> parens $ e1 <> ".wrapping_shl(" <> e2 <> ")"
               | otherwise
               -> parens $ e1 <+> "<<" <+> e2
        BAnd   -> parens $ e1 <+> "&"  <+> e2
        BOr    -> parens $ e1 <+> "|"  <+> e2
        BXor   -> parens $ e1 <+> "^"  <+> e2
        Plus   | uint
               -> parens $ e1 <> ".wrapping_add(" <> e2 <> ")"
               | otherwise
               -> parens $ e1 <+> "+" <+> e2
        Minus  | uint
               -> parens $ e1 <> ".wrapping_sub(" <> e2 <> ")"
               | otherwise
               -> parens $ e1 <+> "-" <+> e2
        Times  | uint
               -> parens $ e1 <> ".wrapping_mul(" <> e2 <> ")"
               | otherwise
               -> parens $ e1 <+> "*" <+> e2
        Concat -> mkConcat (e1, typeWidth t1) (e2, typeWidth t2)
    where
    uint = (not $ isInt d t1) && (typeWidth (typ' d t1) <= 128)

-- These operators require truncating the output value to correct
-- width.
bopsRequireTruncation :: [BOp]
bopsRequireTruncation = [ShiftL, Plus, Minus, Times]

-- Produce code to cast bitvector to a different-width BV.
-- The value of 'e' must fit in the new width.
castBV :: Doc -> Int -> Int -> Doc
castBV e w1 w2 | t1 == t2
               = e
               | w1 <= 128 && w2 <= 128
               = parens $ e <+> "as" <+> t2
               | w2 > 128
               = "Uint::from_" <> t1 <> "(" <> e <> ")"
               | otherwise
               = e <> "to_" <> t2 <> "().unwrap()"
    where
    t1 = mkType $ tBit w1
    t2 = mkType $ tBit w2

-- Concatenate two bitvectors
mkConcat :: (Doc, Int) -> (Doc, Int) -> Doc
mkConcat (e1, w1) (e2, w2) =
    parens $ e1'' <+> "|" <+> e2'
    where
    e1' = castBV e1 w1 (w1+w2)
    e2' = castBV e2 w2 (w1+w2)
    e1'' = parens $ e1' <+> "<<" <+> pp w2

mkSlice :: (Doc, Int) -> Int -> Int -> Doc
mkSlice (e, w) h l = castBV res w (h - l + 1)
    where
    res = parens $ (parens $ e <+> ">>" <+> pp l) <+> "&" <+> mask
    mask = mkBVMask (h - l + 1)

mkBVMask :: Int -> Doc
mkBVMask w | w > 128   = "Uint::parse_bytes(b\"" <> m <> "\", 16)"
           | otherwise = "0x" <> m
    where
    m = pp $ showHex (((1::Integer) `shiftL` w) - 1) ""

mkTruncate :: Doc -> Type -> Doc
mkTruncate v t =
    case t of
         TBit{..}    | needsTruncation typeWidth
                     -> parens $ v <+> "&" <+> mask typeWidth
         _           -> v
    where
    needsTruncation :: Int -> Bool
    needsTruncation w = mask w /= empty
    mask :: Int -> Doc
    mask w | w < 8 || w > 8  && w < 16 || w > 16 && w < 32 || w > 32 && w < 64 || w > 64 && w < 128
           = mkBVMask w
    mask _ = empty

mkInt :: Integer -> Doc
mkInt v | v <= (toInteger (maxBound::Word128)) && v >= (toInteger (minBound::Word128))
        = "Int::from_u128(" <> pp v <> ")"
        | v <= (toInteger (maxBound::Int128))  && v >= (toInteger (minBound::Int128))
        = "Int::from_i128(" <> pp v <> ")"
        | otherwise
        = "Int::parse_bytes(b\"" <> pp v <> "\", 10)"
