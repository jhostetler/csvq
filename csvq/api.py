# LICENSE --------------------------------------------------------------------
# Copyright 2017 Jesse A. Hostetler
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
# ----------------------------------------------------------------------------

""" The primary interface to csvq.
"""

import csvq.aggregate as csvq_aggregate
import csvq.algebra
import csvq.functional
import csvq.relation
import csvq.structure

import builtins # For overload resolution
import contextlib

# ----------------------------------------------------------------------------

class RelationalOperator:
	""" A decorator that enables query composition via the "pipe" operator.
	
	The implementation technique used here is due to the Pipe library:
		https://github.com/JulienPalard/Pipe
	"""
	
	def __init__( self, f ):
		self._f = f
		
	def __call__( self, *args, **kwargs ):
		return RelationalOperator( lambda rel: self._f( rel, *args, **kwargs ) )
		
	def __ror__( self, other ):
		return self._f( other )
		
# ----------------------------------------------------------------------------
# Input and output

@contextlib.contextmanager
def stream( filename, delim=',' ):
	""" Stream an *untyped* relation from a file.
	
	Normally, this function will be used in a `with` statement. The result
	implements the `ContextManager` protocol.
	
	Parameters:
	
	- `filename`: `str`
	- `delim`: `str` [Optional] Column delimiter string
	"""
	r = csvq.relation.FileStreamRelation.open( filename, delim )
	yield r
	r.close()

@contextlib.contextmanager
def stream_typed( filename, delim=',', type_delim=':' ):
	""" Stream a *typed* relation from a file.
	
	Normally, this function will be used in a `with` statement. The result
	implements the `ContextManager` protocol.
	
	A typed relation is specified by using column headers of the form
	`name:type`, where `type` is one of `{str, int, float}`.
	
	Parameters:
	
	- `filename`: `str`
	- `delim`: `str` [Optional] Column delimiter string
	- `type_delim`: `str` [Optional] Type annotation delimiter string
	"""
	r = csvq.relation.FileStreamRelation.open_typed( filename, delim, type_delim )
	yield r
	r.close()
	
def load( filename, delim=',' ):
	""" Load an *untyped* relation from a file into memory.
	
	Unlike `stream()`, the result of `load()` models `RestartableRelation`.
	
	Parameters:
	
	- `filename`: `str`
	- `delim`: `str` [Optional] Column delimiter string
	"""
	with stream( filename, delim ) as input:
		return evaluate( input )
		
def load_typed( filename, delim=',', type_delim=':' ):
	""" Load a *typed* relation from a file into memory.
	
	Unlike `stream()`, the result of `load()` models `RestartableRelation`.
	
	Parameters:
	
	- `filename`: `str`
	- `delim`: `str` [Optional] Column delimiter string
	- `type_delim`: `str` [Optional] Type annotation delimiter string
	"""
	with stream_typed( filename, delim, type_delim ) as input:
		return evaluate( input )
		
def write( relation, out, delim=',' ):
	""" Write a relation to a file stream without type information.
	
	Parameters:
	
	- `relation`: `Relation`
	- `out`: An open writeable file handle
	- `delim`: `str` [Optional] Column delimiter
	"""
	print( delim.join( relation.names() ), file=out )
	write_without_headers( relation, out, delim )

def write_typed( relation, out, delim=',', type_delim=':' ):
	""" Write a relation to a file stream with type information.
	
	Parameters:
	
	- `relation`: `Relation`
	- `out`: An open writeable file handle
	- `delim`: `str` [Optional] Column delimiter
	- `type_delim`: `str` [Optional] Type annotation delimiter
	"""
	typed = [attr.name() + type_delim + attr.type_string() for attr in relation.attributes()]
	print( delim.join( typed ), file=out )
	write_without_headers( relation, out, delim )
		
def write_without_headers( relation, out, delim=',' ):
	""" Write only the tuples of a relation to a file stream.
	
	Parameters:
	
	- `relation`: `Relation`
	- `out`: An open writeable file handle
	- `delim`: `str` [Optional] Column delimiter
	"""
	for t in relation:
		print( delim.join( builtins.map( str, t ) ), file=out )
		
# ----------------------------------------------------------------------------
# Utility

def evaluate( relation ):
	""" Evaluate a relational expression.
	
	The input relation is traversed, and an object that models
	`RestartableRelation` is returned.
	
	Parameters:
	
	- `relation`: `Relation` The relational expression to evaluate.
	"""
	return csvq.relation.InMemoryRelation.copy_of( relation )
	
def sorted( relation, key = None, reverse = False ):
	""" Copies the input relation, sorts the copy, and returns it.
	
	Parameters:
	
	- `key`: `[str]` [Optional] Ordered list of column names for sort key
	- `reverse`: `bool` [Optional] If True, reverse sort order
	"""
	r = csvq.relation.InMemoryRelation.copy_of( relation )
	r.sort( key = key, reverse = reverse )
	return r

# ----------------------------------------------------------------------------
# Relational algebra
		
@RelationalOperator
def project( relation, *keep ):
	""" Retains only those attributes with names in `keep`.
	
	Parameters:
	
	- `keep`: [str] Names of columns to keep
	"""
	return csvq.algebra.Projection( relation, keep )
	
@RelationalOperator
def project_complement( relation, *drop ):
	""" Retains only those attributes with names *not* in `drop`.
	
	Parameters:
	
	- `drop`: `[str]` Names of columns to drop
	"""
	drop_set = set( drop )
	keep = tuple( a.name() for a in relation.attributes() if a.name() not in drop_set )
	return csvq.algebra.Projection( relation, keep )
	
@RelationalOperator
def select( relation, predicate ):
	""" Retains only those tuples that satisfy `predicate`.
	
	Parameters:
	
	- `predicate`: `Row -> bool`
	"""
	return csvq.algebra.Selection( relation, predicate )
	
@RelationalOperator
def rename( relation, substitutions ):
	""" Renames attributes according to a `dict` of substitutions.
	
	Parameters:
	
	- `substitutions`: `{old_name: new_name}`
	"""
	return csvq.algebra.Rename( relation, substitutions )
	
@RelationalOperator
def natural_join( left, right ):
	""" The natural join `left |><| right`.
	
	If one relation is much larger than the other, the larger relation should
	be passed in the `left` parameter for efficiency.
	
	Parameters:
	
	- `left`: The first relation
	- `right`: The second relation. **Consumed immediately.**
	"""
	return csvq.algebra.HashNaturalJoin( left, right )
	
# ----------------------------------------------------------------------------
# Functional programming

@RelationalOperator
def map( relation, fd ):
	""" Map functions of rows into columns.
		
	Parameters:
	
	- `relation`: `Relation`
	- `fd`: `{Attribute: Row -> t}` For each item `attr: f` in `fd`, the output
		relation will have a column labeled with `attr` computed by mapping `f`
		over each row. The return type `t` of `f` must be assignment-compatible
		with the type of the destination attribute.
	"""
	return csvq.functional.Map( relation, fd )
	
@RelationalOperator
def update( relation, fd ):
	""" Map functions of rows into columns in-place.
		
	Parameters:
	
	- `relation`: `Relation`
	- `fd`: `{str: Row -> t}` The output relation will be equal	to `relation`
		except that for each item `name: f` in `fd`, column `name` will be
		altered by mapping `f` over each row. The return type `t` of `f` must
		be assignment-compatible with the type of the named attribute.
	"""
	return csvq.functional.Update( relation, fd )
	
@RelationalOperator
def fold( relation, fd ):
	""" Fold all rows into a single row.
		
	Parameters:
	
	- `fd`: `{Attribute: (t -> Row -> t, t)}` For each item `attr: (f, init)`
		in `fd`, the output relation will have a column labeled with `attr`
		computed by folding `f` over each row starting from initial value
		`init`.
	"""
	return csvq.functional.Fold( relation, fd )
	
# ----------------------------------------------------------------------------

@RelationalOperator
def aggregate( relation, *fs ):
	""" Apply the basic SQL "aggregation" operations.
	
	The default implemented aggregation operations are: `Count`, `Max`, `Mean`,
	`Min`, `Sum`, `Variance`.
	
	Parameters:
	
	- `fs`: `[(str, AggregateType)]` The values in column `name` are
		accumulated using an instance of `AggregateType`, which must implement
		the `csvq.aggregate.Aggregate` concept. The	resulting relation contains
		one column for each aggregation	operation, with name and type as
		specified by `AggregateType`.
	"""
	return csvq_aggregate.Aggregate( relation, fs )

# ----------------------------------------------------------------------------
# Structure manipulation

@RelationalOperator
def hcat( *relations ):
	""" Concatenate relations horizontally.
	
	All relations must have the same number of rows, and there must be no
	duplicate attribute names.
	
	The input relations are **not** consumed immediately.
	"""
	return csvq.structure.HCat( *relations )

@RelationalOperator
def vcat( *relations ):
	""" Concatenate relations vertically.
	
	All relations must have the same attribute set (names and types). The order
	of columns is determined by the first relation in the input list.
	
	The input relations are **not** consumed immediately.
	"""
	return csvq.structure.VCat( *relations )
	
@RelationalOperator
def assign( relation, changes ):
	""" Assign new values to columns.
	
	Parameters:
	
	- `relation`: `Relation` The input relation.
	- `changes`: `Relation` A relation containing a subset of the columns of
		`relation` and having the same number of rows. The corresponding
		columns in `relation` are overwritten by those columns from `changes`.
		`changes` is **not** consumed immediately.
	"""
	return csvq.structure.Assign( relation, changes )
	
@RelationalOperator
def alter_type( relation, td ):
	""" Change the types of attributes.
	
	If the new type is different from the old type, the values in the column
	are converted by applying the new type's constructor.
	
	Parameters:
	
	- `td`: `{str: T}` For each entry `name: type` in `td`, the type of column
		`name` is altered to be `T`, which must be one of `{str, int, float}`.
	"""
	return csvq.structure.AlterType( relation, td )
	
# ----------------------------------------------------------------------------
# Data extraction

def scalar( relation ):
	""" Returns the Python value in the first column of the first row.
	"""
	for t in relation:
		return t[0]
		
def vector( relation ):
	""" Returns a Python tuple containing the Python values in the first row.
	"""
	for t in relation:
		return t

def tuples( relation ):
	""" Returns the entire relation as a Python list of tuples.
	"""
	return [t for t in relation]
