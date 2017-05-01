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

""" The Relation base class, and facilities for populating Relations with data.
"""

import operator

# ----------------------------------------------------------------------------

class Attribute:
	""" Stores the name and type of a "column" in the data.
	
	The default type is 'str'.
	"""
	
	def __init__( self, name, type=str ):
		self._name = name
		self._type = type
		
	@staticmethod
	def parse_type( type ):
		if type == "float":
			return float
		elif type == "int":
			return int
		elif type == "str":
			return str
		else:
			raise KeyError( type )
			
	def type_string( self ):
		if self._type == float:
			return "float"
		elif self._type == int:
			return "int"
		elif self._type == str:
			return "str"
		else:
			raise KeyError( self._type )
		
	def name( self ):
		return self._name
		
	def type( self ):
		return self._type
		
	def __eq__( self, other ):
		return self._name == other._name and self._type == other._type
		
	def __hash__( self ):
		return hash( (self._name, self._type) )
		
# ----------------------------------------------------------------------------

class Row:
	""" Provides a friendly interface to a "row" (== tuple) in the data.
	
	Row instances can be indexed in three ways:
		1. row.ColumnName works if ColumnName is a valid Python identifier
		2. row("Column.Name") works with an arbitrary string
		3. row[i] accesses elements by integer positional index
	Row instances also model Iterable.
	
	Row instances are valid only as long as the backing Relation is in scope.
	"""
	
	def __init__( self, relation, row_data ):
		self._relation = relation
		self._row_data = row_data
		
	def __getattr__( self, name ):
		return self[self._relation.index( name )]
		
	def __call__( self, name ):
		return self[self._relation.index( name )]
		
	def __getitem__( self, index ):
		return self._row_data[index]
		
	def __iter__( self ):
		return iter(self._row_data)
		
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------

class Relation:
	""" The base class for Relations in the relational database model.
	
	Conceptually, a Relation is an iterable collection of tuples, together with
	name and type information (attributes) for each "column".
	
	Sub-classes must model Iterable. Beyond that, we can identify two sub-types
	of Relation:
	  - A Relation is a SinglePassRelation if it models Iterator
	  - A Relation is a RestartableRelation if successive calls to iter()
	    return independent iterators that yield the same sequence of tuples.
	Most queries are implemented as SinglePassRelations, which allows them to
	be evaluated lazily.
	"""
	
	def __init__( self, attributes, index=None ):
		self._attributes = tuple(attributes)
		if index is None:
			self._index = {a.name(): i for (i, a) in enumerate(attributes)}
		else:
			self._index = index
			
	@staticmethod
	def parse_attributes( line, delim=',', type_delim=None ):
		clean = line.rstrip( '\n' )
		attributes = []
		for token in clean.split( delim ):
			if type_delim is None:
				attributes.append( Attribute( token ) )
			else:
				try:
					(name, type) = token.split( type_delim )
					attributes.append( Attribute( name, Attribute.parse_type(type) ) )
				except ValueError:
					raise TypeError( "Malformed type specification: '" + token + "'" )
		return tuple( attributes )
		
	def n_attributes( self ):
		return len(self._attributes)
		
	def attributes( self ):
		return self._attributes
		
	def names( self ):
		for attr in self._attributes:
			yield attr.name()
			
	def types( self ):
		for attr in self._attributes:
			yield attr.type()
			
	def index( self, name ):
		return self._index[name]
		
	def attribute( self, name ):
		return self._attributes[self.index(name)]
		
	def type( self, name ):
		return self.attribute( name ).type()
		
	def rows( self ):
		""" Returns an Iterable yielding a Row instance for each tuple in the
		relation.
		"""
		for t in self:
			yield Row( self, t )
			
	def __eq__( self, other ):
		return (self.attributes() == other.attributes() and
			    [t for t in self] == [t for t in other])
		
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------

class FileStreamRelation(Relation):
	""" A SinglePassRelation backed by a file.
	"""

	def __init__( self, filename, delim=',', type_delim=None ):
		self._file = open( filename )
		self._delim = delim
		self._itr = iter(self._file)
		super().__init__( attributes = Relation.parse_attributes( next(self._itr), delim, type_delim ) )
		
	def close( self ):
		self._file.close()
		
	@staticmethod
	def open( filename, delim=',' ):
		return FileStreamRelation( filename, delim )
	
	@staticmethod
	def open_typed( filename, delim=',', type_delim=':' ):
		return FileStreamRelation( filename, delim, type_delim )
		
	def __iter__( self ):
		return self
	
	def __next__( self ):
		return tuple( self._attributes[i].type()( s )
					  for (i, s) in enumerate( next(self._itr).rstrip( '\n' ).split( self._delim ) ) )

# ----------------------------------------------------------------------------
		
class InMemoryRelation(Relation):
	""" A RestartableRelation backed by data in memory.
	"""
	
	def __init__( self, attributes, data ):
		super().__init__( attributes = attributes )
		self._data = data
		
	def sort( self, key = None, reverse = False ):
		if key is not None:
			idx = tuple( self.index(n) for n in key )
			self._data.sort( key = operator.itemgetter( *idx ), reverse = reverse )
		else:
			self._data.sort( reverse = reverse )
		
	@staticmethod
	def copy_of( relation ):
		cp = list( t for t in relation )
		return InMemoryRelation( attributes = relation._attributes, data = cp )
		
	@staticmethod
	def from_file( filename, delim=',' ):
		with FileStreamRelation.open( filename ) as r:
			return InMemoryRelation.copy_of( r )
	
	@staticmethod
	def from_file_typed( filename, delim=',', type_delim=':' ):
		with FileStreamRelation.open_typed( filename ) as r:
			return InMemoryRelation.copy_of( r )
		
	def __iter__( self ):
		return iter(self._data)
