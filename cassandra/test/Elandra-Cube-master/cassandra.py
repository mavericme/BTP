# module cassandra.py
from pycassa.system_manager import *
from pycassa.pool import ConnectionPool
from pycassa.types import *
from pycassa.columnfamily import *
import json

#wrapper for cassandra
class cassandra_connection:
	
	def __init__(self, keyspace="", table="" , host="localhost:9160"):
		self.keyspace = keyspace
		self.table = table
		self.host = host
		
		sys = SystemManager(self.host)
		
		f = open('schema.json')
		content = f.read()
		f.close()
		
		self.schema = json.loads(content)[keyspace][table]["columns"]
		
		if self.schema == None:
			print "Schema not found"
		
		if keyspace not in sys.list_keyspaces():
			sys.create_keyspace(keyspace, SIMPLE_STRATEGY, {'replication_factor' : '1'})
		
		if table not in sys.get_keyspace_column_families(keyspace):
			validators = {}
			
			for key in self.schema:
				validators[key] = self.type_classes(self.schema[key]["type"])
				
			sys.create_column_family(self.keyspace, 
						 table, 
						 super = False, 
						 comparator_type = UTF8_TYPE,
						 key_validation_classs = INT_TYPE,
						 column_validator_classes = validators)

			
		
#	def connect(self, host):
#		""" Connect to the specified host and persist to object
#			All subsequent operations are perfomed on this connection
#			
#			@args:
#				host(string) : host IP 
#			
#			@ret : boolean (true if connection succesful else false)
#		"""
#		try:
#			SystemManager(host)
#			self.host = host
#			return True
#		except Exception:
#			return False
#		pass
		
#	def create_keyspace(self, keyspace):
#		""" Create a keyspace in the host already created
#			
#			@args:
#				keyspace(string): the keyspace name
#				
#			@ret:
#				boolean (true if successfully created keyspace else false)
#		"""
#		try:
#			sys = SystemManager(self.host)
#			self.keyspace = keyspace
#			sys.create_keyspace(keyspace, SIMPLE_STRATEGY, {'replication_factor' : '1'})
#			return True
#		except Exception:
#		pass
		
	def type_classes(self, x):
		return {
			"text": UTF8_TYPE,
			"int" : INT_TYPE,
		        "str": UTF8_TYPE	
		}[x]
		
#	def create_column_family(self, name, columns, primary_key):
#		""" Create a column family with the given name  and columns
#			and facts as specified
#			
#			@args:
#				name (string) : the name of the column family
#				columns ( { string = string} ) : a dictionary specifying columns
#													key   : column name
#													value : column type
#				primary_key (string) : name of the primary key
#				
#			@ret:
#				boolean (true if column family created, else false)
#		"""
#		f = open('schema.json')
#		content = f.read()
#		
#		sc = json.loads(content)
#		
#		self.schema = sc[keyspace][table]["columns"]
#		
#		ss =  SystemManager(host)
#		
#		try:
#			self.schema = columns
#			
#			validators = {}
#			
#			for key in columns:
#				validators[key] = self.type_classes(columns[key])
#			
#			#print self.validators
#			
#			sys = SystemManager(self.host)
#			
#			self.table = name
#			sys.create_column_family(self.keyspace, 
#						 name, 
#						 super = False, 
#						 comparator_type = UTF8_TYPE,
#						 key_validation_classs = INT_TYPE,
#						 column_validator_classes = validators)
#			return True
#		except Exception as inst:
#			#print inst
#			return False
#		pass
		
	def get_iterator(self):
		pool = ConnectionPool(self.keyspace, [self.host])
		col_fam = ColumnFamily(pool, self.table)
		return col_fam.get_range()
		
	def get_schema(self):
		return self.schema
		pass
	
	def insert(self, key, items):
		try:
			pool = ConnectionPool(self.keyspace, [self.host])
			col_fam = ColumnFamily(pool, self.table)
			col_fam.insert(key, items)
			return True
		except Exception as inst:
			#print inst
			return False
		pass
	
################# TESTING ###########################
def test():
	cc = cassandra_connection("k1", "t1")		
	
	print cc.insert("0", {"fname":"abcd", "lname": "xyz", "marks": "22"})
	print cc.insert("1", {"fname":"lmno", "lname": "pqr", "marks": "10"})
	
	for x in cc.get_iterator():
		print x

#test()

########### Adeel 19:24 10/11/2013 - Resolved ######################
# Now you don't have to explicitly create the keyspaces and column families
# just specify the names of the keyspace and coulmn family you want to connect to
# at the time of the object construction. The keyspace and the table 
# will be created if they don't exist. Now you can do other operations on this connection.


############ Feature requests ##################
# please remove the need to do cc.create_keyspace, cc.create_column_family everytime
# so that we can do cc.get_iterator() directly

# also, save schema in a file so that table={...} and other things are not necessary and we can directly do getSchema()
# please try to use schema.json format (file in same directory)

#--- fixed above this :D----
# data returned is all str()... can we do something about it?
