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

import os
import statistics
import unittest

from csvq import *

class TestApi(unittest.TestCase):
	""" Basic unit tests for the csvq.api module.
	
	We don't explicitly test the Relation objects that implement the API, but
	these should be covered by the API tests.
	"""
	
	# ------------------------------------------------------------------------
	
	def setUp( self ):
		self.employees = load_typed( "examples/employees-typed.csv" )
		
	def assert_equal_attributes( self, r, s ):
		self.assertEqual( r.attributes(), s.attributes() )
		
	# ------------------------------------------------------------------------
		
	def test_load( self ):
		r = load( "examples/employees-untyped.csv" )
		attrs = r.attributes()
		self.assertEqual( r.n_attributes(), 5 )
		self.assertEqual( r.n_attributes(), len(attrs) )
		self.assertEqual( attrs[0].type(), str )
		self.assertEqual( attrs[1].type(), str )
		self.assertEqual( attrs[2].type(), str )
		self.assertEqual( attrs[3].type(), str )
		self.assertEqual( attrs[4].type(), str )
		self.assertEqual( len(tuples( r )), 6 )
		
	def test_load_typed( self ):
		r = load_typed( "examples/employees-typed.csv" )
		attrs = r.attributes()
		self.assertEqual( r.n_attributes(), 5 )
		self.assertEqual( r.n_attributes(), len(attrs) )
		self.assertEqual( attrs[0].type(), str )
		self.assertEqual( attrs[1].type(), int )
		self.assertEqual( attrs[2].type(), str )
		self.assertEqual( attrs[3].type(), str )
		self.assertEqual( attrs[4].type(), float )
		self.assertEqual( len(tuples( r )), 6 )
		
	def test_load_typed_from_untyped( self ):
		# Note: This test causes a ResourceWarning for 'unclosed file'. I
		# believe this is spurious.
		with self.assertRaises( TypeError ):
			r = load_typed( "examples/employees-untyped.csv" )
		
	def test_stream_once( self ):
		with stream( "examples/employees-untyped.csv" ) as s:
			e = evaluate( s )
			f = evaluate( s )
			self.assertEqual( len(tuples(f)), 0 )
		
	def test_stream_typed_once( self ):
		with stream_typed( "examples/employees-typed.csv" ) as s:
			e = evaluate( s )
			f = evaluate( s )
			self.assertEqual( len(tuples(f)), 0 )
			
	def test_stream_typed_from_untyped( self ):
		with self.assertRaises( TypeError ):
			with stream_typed( "examples/employees-untyped.csv" ) as s:
				r = evaluate( s )
				
	def test_write( self ):
		try:
			tmp_file = "__tmp_employees.csv"
			untyped = load( "examples/employees-untyped.csv" )
			with open( tmp_file, "w" ) as out:
				write( untyped, out )
			cp = load( tmp_file )
			self.assertEqual( untyped, cp )
		finally:
			os.remove( tmp_file )
	
	def test_write_typed( self ):
		try:
			tmp_file = "__tmp_employees.csv"
			with open( tmp_file, "w" ) as out:
				write_typed( self.employees, out )
			cp = load_typed( tmp_file )
			self.assertEqual( self.employees, cp )
		finally:
			os.remove( tmp_file )
		
	# ------------------------------------------------------------------------

	def test_project( self ):
		r = evaluate( self.employees | project( "Name", "Age" ) )
		self.assertEqual( [a.name() for a in r.attributes()], ["Name", "Age"] )
		
	def test_project_complement( self ):
		r = evaluate( self.employees | project_complement( "Name", "Age" ) )
		self.assertEqual( [a.name() for a in r.attributes()], ["Sex", "Title", "Salary"] )
	
	def test_project_empty( self ):
		r = evaluate( self.employees | project() )
		self.assertEqual( r.attributes(), () )
		# TODO: Would we rather have this produce an empty relation?
		self.assertEqual( tuples( r ), [()] * len(tuples(self.employees)) )
	
	def test_project_nonexistent( self ):
		with self.assertRaises( KeyError ):
			r = evaluate( self.employees | project( "Nonexistent" ) )
		
	def test_select( self ):
		r = evaluate( self.employees | select( lambda t: t.Age == 10 ) )
		ts = tuples( r )
		self.assert_equal_attributes( self.employees, r )
		self.assertEqual( ts[0], ("Bart", 10, "M", "Policeman", 20000.0) )
		self.assertEqual( ts[1], ("Martin", 10, "M", "Systems analyst", 40000.0) )
		
	def test_select_empty( self ):
		r = evaluate( self.employees | select( lambda t: False ) )
		self.assert_equal_attributes( self.employees, r )
		self.assertEqual( len(tuples(r)), 0 )
		
	def test_rename( self ):
		i = self.employees.index( "Name" )
		t = self.employees.attribute( "Name" ).type()
		r = evaluate( self.employees | rename( {"Name": "EmployeeName"} ) )
		self.assertEqual( i, r.index( "EmployeeName" ) )
		with self.assertRaises( KeyError ):
			r.index( "Name" )
		self.assertEqual( t, r.attribute( "EmployeeName" ).type() )
		
	def test_natural_join( self ):
		# FIXME: This test case generates:
		# 	PendingDeprecationWarning: generator '__iter__' raise StopIteration
		# Presumably this is caused by the implementation of
		# algebra.HashNaturalJoin because it does no occur in previous tests.
		# I don't know how to fix it right now.
		parents = load_typed( "examples/parents-typed.csv" )
		r = evaluate( self.employees | rename( {"Name": "ChildName"} ) | natural_join( parents ) )
		ts = tuples( r )
		self.assertEqual( [a.name() for a in r.attributes()],
						  ["ChildName", "Age", "Sex", "Title", "Salary", "FatherName", "MotherName"] )
		self.assertEqual( [a.type() for a in r.attributes()],
						  [str, int, str, str, float, str, str] )
		self.assertEqual( len(ts), 4 )
		self.assertEqual( ts[0], ("Lisa", 8, "F", "Homemaker", 0.0, "Homer", "Marge") )
		self.assertEqual( ts[1], ("Bart", 10, "M", "Policeman", 20000.0, "Homer", "Marge") )
		self.assertEqual( ts[2], ("Ralph", 8, "M", "Salmon gutter", 10000.0, "Clancy", "Sheila") )
		self.assertEqual( ts[3], ("Milhouse", 9, "M", "Military strongman", 50000.0, "Kurt", "Louise") )
		
	# ------------------------------------------------------------------------
		
	def test_map( self ):
		r = evaluate( self.employees | map( {Attribute("MonthlySalary", float) : lambda t: t.Salary / 12.0} ) )
		ts = tuples(self.employees)
		rs = tuples(r)
		i = self.employees.index( "Salary" )
		for ti in range(0, len(ts)):
			with self.subTest( i = ti ):
				self.assertEqual( ts[ti][i] / 12.0, rs[ti][0] )
				
	def test_update( self ):
		r = evaluate( self.employees | update( {"Salary" : lambda t: t.Salary + 10000.0} ) )
		ts = tuples(self.employees)
		rs = tuples(r)
		i = self.employees.index( "Salary" )
		self.assert_equal_attributes( self.employees, r )
		for ti in range(0, len(ts)):
			with self.subTest( i = ti ):
				self.assertEqual( ts[ti][i] + 10000.0, rs[ti][i] )
				
	def test_fold( self ):
		r = evaluate( self.employees | fold( {Attribute("LifetimeEarnings", float) 
												: (lambda acc, t: acc + t.Salary * t.Age, 0)} ) )
		check = 0
		for row in self.employees.rows():
			check += row.Salary * row.Age
		self.assertEqual( r.n_attributes(), 1 )
		self.assertEqual( r.attribute( "LifetimeEarnings" ).type(), float )
		self.assertEqual( len(tuples(r)), 1 )
		self.assertEqual( check, scalar(r) )
		
	# ------------------------------------------------------------------------
		
	def test_aggregate( self ):
		r = evaluate( self.employees | aggregate( *[("Salary", op) for op in [Count, Max, Mean, Min, Sum, Variance]] ) )
		salaries = evaluate( self.employees | project( "Salary" ) )
		data = [t[0] for t in salaries]
		self.assertEqual( [a.name() for a in r.attributes()],
						  ["Salary_Count", "Salary_Max", "Salary_Mean", "Salary_Min", "Salary_Sum", "Salary_Variance"] )
		self.assertEqual( [a.type() for a in r.attributes()],
						  [int, float, float, float, float, float] )
		rv = vector( r )
		self.assertEqual( rv[0], len(data) )
		self.assertEqual( rv[1], max(data) )
		self.assertEqual( rv[2], statistics.mean(data) )
		self.assertEqual( rv[3], min(data) )
		self.assertEqual( rv[4], sum(data) )
		self.assertEqual( rv[5], statistics.variance(data) )
		
	# ------------------------------------------------------------------------
		
	def test_hcat( self ):
		name = evaluate( self.employees | project( "Name" ) )
		age = evaluate( self.employees | project( "Age" ) )
		r = evaluate( name | hcat( age ) )
		self.assertEqual( [a.name() for a in r.attributes()], ["Name", "Age"] )
		self.assertEqual( [a.type() for a in r.attributes()], [str, int] )
		ts = tuples( self.employees )
		rs = tuples( r )
		iname = self.employees.index( "Name" )
		iage = self.employees.index( "Age" )
		self.assertEqual( len(ts), len(rs) )
		for i in range(0, len(ts)):
			with self.subTest( i = i ):
				self.assertEqual( ts[i][iname], rs[i][0] )
				self.assertEqual( ts[i][iage], rs[i][1] )
				
	def test_hcat_duplicate( self ):
		with self.assertRaises( TypeError ):
			r = evaluate( self.employees | hcat( self.employees ) )
	
	def test_hcat_unequal_count( self ):
		parents = load_typed( "examples/parents-typed.csv" )
		with self.assertRaises( RuntimeError ):
			r = evaluate( self.employees | hcat( parents ) )
				
	def test_vcat( self ):
		r = evaluate( self.employees | vcat( self.employees ) )
		self.assert_equal_attributes( self.employees, r )
		ts = tuples( self.employees )
		rs = tuples( r )
		ri = 0
		for i in range(0, len(ts)):
			with self.subTest( i = ri ):
				self.assertEqual( ts[i], rs[ri] )
			ri += 1
		for i in range(0, len(ts)):
			with self.subTest( i = ri ):
				self.assertEqual( ts[i], rs[ri] )
			ri += 1
			
	def test_vcat_incompatible( self ):
		parents = load_typed( "examples/parents-typed.csv" )
		with self.assertRaises( TypeError ):
			r = evaluate( self.employees | vcat( parents ) )
			
	def test_assign( self ):
		increment = 100.0
		m = evaluate( self.employees | update( {"Salary": lambda t: t.Salary + increment} ) )
		r = evaluate( self.employees | assign( m ) )
		self.assert_equal_attributes( self.employees, r )
		ts = tuples( self.employees )
		rs = tuples( r )
		isalary = self.employees.index( "Salary" )
		for i in range(0, len(ts)):
			with self.subTest( i = i ):
				self.assertEqual( rs[i][isalary], ts[i][isalary] + increment )
	
	def test_assign_key_error( self ):
		m = evaluate( self.employees | project( "Name" ) | rename( {"Name": "Nombre"} ) )
		with self.assertRaises( KeyError ):
			r = evaluate( self.employees | assign( m ) )
			
	def test_assign_incompatible( self ):
		m = evaluate( self.employees | project( "Salary" ) | alter_type( {"Salary": int} ) )
		with self.assertRaises( TypeError ):
			r = evaluate( self.employees | assign( m ) )
			
	def test_assign_unequal_count( self ):
		m = evaluate( self.employees | project( "Salary" ) | select( lambda t: t.Salary < 20000.0 ) )
		with self.assertRaises( RuntimeError ):
			r = evaluate( self.employees | assign( m ) )
			
	def test_alter_type_truncate( self ):
		float_increment = 100.5
		m = evaluate( self.employees | update( {"Salary": lambda t: t.Salary + float_increment} ) )
		r = evaluate( m | alter_type( {"Salary": int} ) )
		self.assertEqual( r.attribute( "Salary" ).type(), int )
		ts = tuples( self.employees )
		rs = tuples( r )
		isalary = self.employees.index( "Salary" )
		for i in range(0, len(ts)):
			with self.subTest( i = i ):
				self.assertEqual( int( ts[i][isalary] + float_increment ), rs[i][isalary] )
				
	def test_alter_type_to_str( self ):
		r = evaluate( self.employees | alter_type( {"Salary": str} ) )
		self.assertEqual( r.attribute( "Salary" ).type(), str )
		ts = tuples( self.employees )
		rs = tuples( r )
		isalary = self.employees.index( "Salary" )
		for i in range(0, len(ts)):
			with self.subTest( i = i ):
				self.assertEqual( str( ts[i][isalary] ), rs[i][isalary] )
				
	# ------------------------------------------------------------------------
	
	def test_scalar( self ):
		self.assertEqual( scalar( self.employees ), "Lisa" )
		
	def test_vector( self ):
		self.assertEqual( vector( self.employees ), ("Lisa", 8, "F", "Homemaker", 0.0) )
		
	# ------------------------------------------------------------------------
	
	def test_sort_key( self ):
		expect = [("Janey", 8), ("Lisa", 8), ("Ralph", 8), ("Milhouse", 9)]
		r = evaluate( self.employees | select( lambda t: t.Age < 10 ) | project( "Name", "Age" ) )
		r.sort( key = ["Age", "Name"] )
		ts = tuples(r)
		self.assertEqual( len(ts), len(expect) )
		for i in range(0, len(ts)):
			with self.subTest( i = i ):
				self.assertEqual( expect[i], ts[i] )
		expect.reverse()
		r = sorted( r, key = ["Age", "Name"], reverse = True )
		ts = tuples(r)
		for i in range(0, len(ts)):
			with self.subTest( i = i ):
				self.assertEqual( expect[i], ts[i] )
