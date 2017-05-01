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

from csvq.relation import Attribute, Relation

# ----------------------------------------------------------------------------

class Aggregate(Relation):
	def __init__( self, in_relation, aggregators ):
		def attr( n, f ):
			input_type = in_relation.attribute( n ).type()
			name = n + "_" + f.__name__
			type = f.value_type( input_type )
			return Attribute( name, type )
		attributes = tuple( attr( n, t ) for (n, t) in aggregators )
		super().__init__( attributes = attributes )
		self._itr = iter(in_relation)
		self._fs = tuple( (in_relation.index(n), t()) for (n, t) in aggregators )
		
	def __iter__( self ):
		for t in self._itr:
			for (i, f) in self._fs:
				f( t[i] )
		yield tuple( f.value() for (i, f) in self._fs )
		
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
		
class Count:
	def __init__( self ):
		self._n = 0
		
	def __call__( self, x ):
		self._n += 1
		
	@staticmethod
	def value_type( input_type ):
		return int
		
	def value( self ):
		return self._n
		
# ----------------------------------------------------------------------------
		
class Max:
	def __init__( self ):
		self._max = None
		
	def __call__( self, x ):
		if self._max is None:
			self._max = x
		else:
			self._max = max( self._max, x )
		
	@staticmethod
	def value_type( input_type ):
		return input_type
		
	def value( self ):
		return self._max
		
# ----------------------------------------------------------------------------
		
class Mean:
	def __init__( self ):
		self._m = 0.0
		self._n = 0
		
	def __call__( self, x ):
		self._n += 1
		self._m += (x - self._m) / self._n
		
	@staticmethod
	def value_type( input_type ):
		return input_type
		
	def value( self ):
		return self._m
		
# ----------------------------------------------------------------------------
		
class Min:
	def __init__( self ):
		self._min = None
		
	def __call__( self, x ):
		if self._min is None:
			self._min = x
		else:
			self._min = min( self._min, x )
		
	@staticmethod
	def value_type( input_type ):
		return input_type
		
	def value( self ):
		return self._min

# ----------------------------------------------------------------------------

class Sum:
	def __init__( self ):
		self._sum = 0
		
	def __call__( self, x ):
		self._sum += x
	
	@staticmethod
	def value_type( input_type ):
		return input_type
	
	def value( self ):
		return self._sum
		
# ----------------------------------------------------------------------------
		
class Variance:
	def __init__( self ):
		self._mean = 0.0
		self._n = 0
		self._m2 = 0.0
		
	def __call__( self, x ):
		self._n += 1
		d = x - self._mean
		self._mean += d / self._n
		d2 = x - self._mean
		self._m2 += d*d2
		
	@staticmethod
	def value_type( input_type ):
		return input_type
		
	def value( self ):
		if self._n == 0:
			return float('nan')
		elif self._n == 1:
			return 0.0
		else:
			return self._m2 / (self._n - 1)
