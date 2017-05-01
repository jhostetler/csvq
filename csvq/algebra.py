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

""" The primitive operations of the relational algebra.
"""

from csvq.relation import Attribute, Relation, Row

# ----------------------------------------------------------------------------

class Projection(Relation):
	def __init__( self, in_relation, names ):
		super().__init__( attributes = [in_relation.attribute( name ) for name in names] )
		self._itr = iter(in_relation)
		self._keep = tuple( in_relation.index(a.name()) for a in self._attributes )
		
	def __next__( self ):
		t = next(self._itr)
		return tuple( t[i] for i in self._keep )
		
	def __iter__( self ):
		return self

# ----------------------------------------------------------------------------

class Selection(Relation):
	def __init__( self, in_relation, predicate ):
		super().__init__( attributes = in_relation._attributes, index = in_relation._index )
		self._itr = iter(in_relation)
		self._predicate = predicate
		
	def __next__( self ):
		while True:
			t = next(self._itr)
			if self._predicate( Row( self, t ) ):
				return t
				
	def __iter__( self ):
		return self
		
# ----------------------------------------------------------------------------

class Rename(Relation):
	def __init__( self, in_relation, substitutions ):
		renamed = []
		for a in in_relation._attributes:
			try:
				renamed.append( Attribute( substitutions[a.name()], a.type() ) )
			except KeyError:
				renamed.append( a )
		super().__init__( attributes = renamed )
		self._itr = iter(in_relation)
		
	def __next__( self ):
		return next(self._itr)
				
	def __iter__( self ):
		return self
		
# ----------------------------------------------------------------------------

# TODO: Should implement "join algorithm" separately and provide it as 
# parameter to "join type" class. So, NaturalJoin would construct a join 
# predicate and delegate to a generic HashJoin implementation.
class HashNaturalJoin(Relation):
	""" The natural join L |><| R of two relations L and R.
	
	The output tuples take the form:
		(ordered columns in L):(ordered columns of R not in L).
	
	The algorithm is a "hash join". The entire relation R is read into a dict,
	then each row of L is checked for membership. If efficiency is important
	and one relation is much larger than the other, the *larger* relation
	should be L and the *smaller* relation should be R.
	"""

	def __init__( self, left, right ):
		attr_intersection = []
		attr_union = list(left.attributes())
		self._idx_right = []
		left_names = set(left.names())
		for (i, r) in enumerate(right.attributes()):
			if r.name() not in left_names:
				attr_union.append( r )
				self._idx_right.append( i )
			elif r.type() != left.attribute( r.name() ).type():
				raise TypeError( "Incompatible types for '" + r.name() + "'" )
			else:
				attr_intersection.append( r )
		super().__init__( attributes = attr_union )
		
		# Indices of the key set in both relations. Only 'left' is a member
		# because we're going to compute the hash table for 'right' immediately
		self._left_key	= [left.index( a.name() ) for a in attr_intersection]
		right_key		= [right.index( a.name() ) for a in attr_intersection]
		# Read 'right' into hash table
		self._right_hash = dict()
		for t in right:
			key = tuple( t[i] for i in right_key )
			try:
				v = self._right_hash[key]
			except KeyError:
				v = []
				self._right_hash[key] = v
			v.append( t )
			
		# Lazy iteration over 'left'
		self._itr = iter(left)
		
	def _join_row( self, l, r ):
		j = list(l)
		for i in self._idx_right:
			j.append( r[i] )
		return tuple(j)
		
	def __iter__( self ):
		while True:
			l = next(self._itr)
			key = tuple( l[i] for i in self._left_key )
			try:
				rs = self._right_hash[key]
				for r in rs:
					yield self._join_row( l, r )
			except KeyError:
				pass

# ----------------------------------------------------------------------------
