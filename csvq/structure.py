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

import collections
import itertools

from csvq.relation import Attribute, Relation, Row

# ----------------------------------------------------------------------------

class HCat(Relation):
	def __init__( self, r1, r2, *rest ):
		relations = [r1, r2, *rest]
		name_type = collections.OrderedDict()
		for r in relations:
			for n in r.names():
				if n in name_type:
					raise TypeError( "Duplicate column name '" + n + "'" )
				else:
					name_type[n] = r.type( n )
		
		attrs = tuple( Attribute(name, type) for (name, type) in name_type.items() )
		super().__init__( attributes = attrs )
		self._itrs = tuple( iter(r) for r in  relations )
		
	def __next__( self ):
		ts = tuple( next(itr, None) for itr in self._itrs )
		stopped = tuple( t is None for t in ts )
		if any(stopped):
			if not all(stopped):
				raise RuntimeError( "Unequal tuple counts" )
			else:
				raise StopIteration()
		return tuple( e for t in ts for e in t )
		
	def __iter__( self ):
		return self

# ----------------------------------------------------------------------------

class VCat(Relation):
	def __init__( self, r1, r2, *rest ):
		relations = [r1, r2, *rest]
		attr_set = set( r1.attributes() )
		for r in relations[1:]:
			if attr_set != set( r.attributes() ):
				raise TypeError( "Incompatible attribute sets" )
		super().__init__( attributes = r1.attributes() )
		self._itr = itertools.chain.from_iterable( iter(r) for r in relations )
		
	def __next__( self ):
		return next( self._itr )
		
	def __iter__( self ):
		return self
		
# ----------------------------------------------------------------------------

class Assign(Relation):
	def __init__( self, relation, changes ):
		for attr in changes.attributes():
			if relation.attribute( attr.name() ) != attr:
				raise TypeError( "Incompatible attribute sets" )
		super().__init__( attributes = relation._attributes, index = relation._index )
		self._idx = tuple( (relation.index(name), changes.index(name)) for name in changes.names() )
		self._ritr = iter(relation)
		self._citr = iter(changes)
		
	def __next__( self ):
		tr = next(self._ritr, None)
		tc = next(self._citr, None)
		if tr is None or tc is None:
			if tr is None and tc is None:
				raise StopIteration()
			else:
				raise RuntimeError( "Unequal tuple counts" )
		u = list(tr)
		for (r, c) in self._idx:
			u[r] = tc[c]
		return tuple(u)
		
	def __iter__( self ):
		return self

# ----------------------------------------------------------------------------

class AlterType(Relation):
	def __init__( self, relation, td ):
		idx = []
		attrs = list( relation.attributes() )
		for (i, a) in enumerate(attrs):
			if a.name() in td:
				attrs[i] = Attribute( a.name(), td[a.name()] )
				idx.append( i )
		super().__init__( attributes = attrs )
		self._idx = tuple( idx )
		self._itr = iter(relation)
		
	def __next__( self ):
		t = list( next(self._itr) )
		for i in self._idx:
			t[i] = self._attributes[i].type()( t[i] )
		return tuple( t )
		
	def __iter__( self ):
		return self
