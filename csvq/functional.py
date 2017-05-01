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

from csvq.relation import Attribute, Relation, Row

# ----------------------------------------------------------------------------

class Map(Relation):
	def __init__( self, in_relation, fd ):
		super().__init__( attributes = tuple(fd) )
		self._in_relation = in_relation
		self._itr = iter(in_relation)
		self._fs = tuple( fd.values() )
		
	def __next__( self ):
		t = next(self._itr)
		return tuple( f( Row( self._in_relation, t ) ) for f in self._fs )
		
	def __iter__( self ):
		return self
		
# ----------------------------------------------------------------------------

class Update(Relation):
	def __init__( self, in_relation, fd ):
		# for k in fd:
			# if in_relation.attribute( k.name() ) != k:
				# raise TypeError( "Incompatible attribute definitions" )
		super().__init__( attributes = in_relation._attributes, index = in_relation._index )
		self._itr = iter(in_relation)
		self._idx = tuple( self.index( k ) for k in fd )
		self._fs = tuple( fd.values() )
		
	def __next__( self ):
		t = next(self._itr)
		tprime = list( t )
		for (i, f) in enumerate(self._fs):
			tprime[self._idx[i]] = f( Row( self, t ) )
		return tuple( tprime )
		
	def __iter__( self ):
		return self
		
# ----------------------------------------------------------------------------

class Fold(Relation):
	def __init__( self, in_relation, fd ):
		super().__init__( attributes = tuple(fd) )
		self._in_relation = in_relation
		self._fs = tuple( f for (f, v) in fd.values() )
		self._v  = list( v for (f, v) in fd.values() )
		
	def __iter__( self ):
		for t in self._in_relation:
			for i in range(0, len(self._v)):
				self._v[i] = self._fs[i]( self._v[i], Row( self._in_relation, t ) )
		yield tuple( self._v )
