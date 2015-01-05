
import pycassa
import bisect
import hashlib
import sys

from collections import OrderedDict
from pycassa.system_manager import *
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily	
	
from flask import Flask
from flask import render_template, request, redirect, url_for

app = Flask(__name__)


@app.route("/", methods=['GET', 'POST'])
def main():
	output = keyspace()
	#output.keyspace_columnfamily_list("localhost:9160")
	return render_template('home.html')


@app.route('/home/', methods=['GET', 'POST'])
def index(name=None):
    return render_template('home.html', name=name)

@app.route('/keyspace/', methods=['GET', 'POST'])
def keyspace(name=None):
    return render_template('keyspace.html', name=name)

@app.route('/column_family/', methods=['GET', 'POST'])
def column_family(name=None):
    return render_template('column_family.html', name=name)
    
@app.route('/data_manipulation/', methods=['GET', 'POST'])
def data_manipulation(name=None):
    return render_template('data_manipulation.html', name=name)
    
@app.route('/data_search/', methods=['GET', 'POST'])
def data_search(name=None):
    return render_template('data_search.html', name=name)

@app.route('/cluster_operations/', methods=['GET', 'POST'])
def cluster_operations(name=None):
    return render_template('cluster_operations.html', name=name)
    
#    if request.method == 'POST':
#    	if 'submit' in request.form:
#    		return redirect(url_for('index'))

#---------------------------------------------------------------------------------------------------------------------------------------
@app.route('/Create_Keyspace/', methods=['GET', 'POST'])
def Create_Keyspace():
    	if request.method == 'POST':
		ks_name = request.form["1ks_name"]
		replication = request.form["2replication"]
		if ks_name and replication:
			return Create_Keyspace_output(ks_name,replication)
		else:
			return Create_Keyspace_input("Please enter valid keyspace name and replication factor")
	elif request.method == 'GET':
		return Create_Keyspace_input()

def Create_Keyspace_input(error = ""):
	inputs = { "1ks_name":"Keyspace Name" , "2replication":"Replication Factor" }
	info = { "title" : "Create Keyspace",
		 "description" : "A simple inquiry of function.", 
		 "error" : error }	
	return render_template('1.html', info = info , inputs = inputs )

def Create_Keyspace_output(ks_name,replication):
	output = keyspace()
	result = output.keyspace_create('localhost:9160', ks_name,replication)	
	#result = "Successfully Created"
	info = { "title" : "Output for Create Keyspace",
		 "description" : "A simple inquiry of function." }
	return render_template('1_res.html', info = info , result = result)   
#---------------------------------------------------------------------------------------------------------------------------------------	 

@app.route('/Drop_Keyspace/', methods=['GET', 'POST'])
def Drop_Keyspace():
	if request.method == 'POST':
		ks_name = request.form["1ks_name"]
		if ks_name:
			return Drop_Keyspace_output(ks_name)
		else:
			return Drop_Keyspace_input("Please enter valid keyspace name")
	elif request.method == 'GET':
		return Drop_Keyspace_input()
			
def Drop_Keyspace_input(error = ""):
	inputs = {  "1ks_name":"Keyspace Name" }
	info = { "title" : "Drop Keyspace",
                 "description" : "A simple inquiry of function.",
		 "error" : error }	
	return render_template('2.html', info = info , inputs = inputs )
	
def Drop_Keyspace_output(ks_name):
	output = keyspace()
	result = output.keyspace_delete('localhost:9160', ks_name)
	#result = "Successfully Droped"
	info = { "title" : "Output for Create Drop keyspace",
		 "description" : "A simple inquiry of function." }
	return render_template('2_res.html', info = info , result = result ) 
#---------------------------------------------------------------------------------------------------------------------------------------

@app.route('/List_Keyspace/', methods=['GET', 'POST'])
def List_Keyspace(error = ""):
	output = keyspace()
	result = output.keyspace_get_list('localhost:9160')
	#result = ['TK1', 'system', 'testks']	
	info = { "title" : "Output for List Keyspace",
		 "description" : "A simple inquiry of function.", 
		 "error" : error }
	return render_template('3_res.html', info = info , result = result )
#---------------------------------------------------------------------------------------------------------------------------------------

@app.route('/Create_ColumnFamily/', methods=['GET', 'POST'])
def Create_ColumnFamily():
    	if request.method == 'POST':
		ks_name = request.form["1ks_name"]
		cf_name = request.form["2cf_name"]
		if ks_name and cf_name:
			return Create_ColumnFamily_output(ks_name,cf_name)
		else:
			return Create_ColumnFamily_input("Please enter valid keyspace name and column family name")
	elif request.method == 'GET':
		return Create_ColumnFamily_input()

def Create_ColumnFamily_input(error = ""):
	inputs = { "1ks_name":"Keyspace Name" ,"2cf_name":"Column Family Name" }
	info = { "title" : "Output for Create Column-Family",
		 "description" : "A simple inquiry of function.", 
		 "error" : error }	
	return render_template('4.html', info = info , inputs = inputs )

def Create_ColumnFamily_output(ks_name,cf_name):
	output = keyspace()
	result = output.colum_family_create('localhost:9160', ks_name, cf_name)
	#result = "Successfully Created"
	info = { "title" : "Output for Create Column-Family",
		 "description" : "A simple inquiry of function."  }
	return render_template('4_res.html', info = info , result = result )  
#---------------------------------------------------------------------------------------------------------------------------------------

@app.route('/Drop_ColumnFamily/', methods=['GET', 'POST'])
def Drop_ColumnFamily(name=None):
	if request.method == 'POST':
		ks_name = request.form["1ks_name"]
		cf_name = request.form["2cf_name"]
    		if ks_name and cf_name:
			return Drop_ColumnFamily_output(ks_name,cf_name)
		else:
			return Drop_ColumnFamily_input("Please enter valid keyspace and column family name")
	elif request.method == 'GET':
		return Drop_ColumnFamily_input()
		
def Drop_ColumnFamily_input(error = ""):
	inputs = { "1ks_name":"Keyspace Name" , "2cf_name":"Column Family Name" }
	info = { "title" : "Drop Column-Family",
                 "description" : "A simple inquiry of function.",
		 "error" : error }	
	return render_template('5.html', info = info , inputs = inputs )
def Drop_ColumnFamily_output(ks_name,cf_name):
	output = keyspace()
	result = output.colum_family_delete('localhost:9160', ks_name, cf_name)
	#result = "Successfully Deleted"
	info = { "title" : "Output for Drop Column-Family",
		 "description" : "A simple inquiry of function." }
	return render_template('5_res.html', info = info , result = result )
#---------------------------------------------------------------------------------------------------------------------------------------

@app.route('/List_ColumnFamily/', methods=['GET', 'POST'])
def List_ColumnFamily():
	if request.method == 'POST':
		ks_name = request.form["1ks_name"]
    		if ks_name:
			return List_ColumnFamily_output(ks_name)
		else:
			return List_ColumnFamily_input("Please enter valid keyspace name")
	elif request.method == 'GET':
		return List_ColumnFamily_input()
		
def List_ColumnFamily_input(error = ""):
	inputs = { "1ks_name":"Keyspace Name" }
	info = { "title" : "List Column-Family",
                 "description" : "A simple inquiry of function.",
		 "error" : error }	
	return render_template('6.html', info = info , inputs = inputs )
def List_ColumnFamily_output(ks_name):
	output = keyspace()
	result = output.colum_family_list('localhost:9160', ks_name)
	#print result
	#result = ['TestCF','Testcf2']	
	info = { "title" : "Output for Column-Family list",
		 "description" : "A simple inquiry of function." }
	return render_template('6_res.html', info = info , result = result )
#---------------------------------------------------------------------------------------------------------------------------------------

@app.route('/Add_Entry_ColumnFamily/', methods=['GET', 'POST'])
def Add_Entry_ColumnFamily():
	if request.method == 'POST':
		ks_name = request.form["1ks_name"]
		cf_name = request.form["2cf_name"]
		key = request.form["3primary_key"]
		key_1 = request.form["k_0"]
		value_1 = request.form["v_0"]
		count = int(request.form["count"])
		dic1 = {}
		dic2 = OrderedDict()
		print count
    		if ks_name and cf_name and key and key_1 and value_1 and count>=0:    			
    			for i in range(count+1):
    				key_t = request.form["k_" + str(i)]
				value_t = request.form["v_" + str(i)]
				if key_t and value_t:
					dic2[str(key_t)] = str(value_t)
			dic1[str(key)] = dic2
			#print dic1
			return Add_Entry_ColumnFamily_output(ks_name,cf_name,dic1)
		else:
			return Add_Entry_ColumnFamily_input("Please enter keyspace, column family & content")
	elif request.method == 'GET':
		return Add_Entry_ColumnFamily_input()
		
def Add_Entry_ColumnFamily_input(error = ""):
	inputs = { "1ks_name":"Keyspace Name" , "2cf_name":"Column Family Name" , "3primary_key":"Primary key" }
	info = { "title" : "Add Entry to Column-Family",
                 "description" : "A simple inquiry of function.",
		 "error" : error }	
	return render_template('7.html', info = info , inputs = inputs )
def Add_Entry_ColumnFamily_output(ks_name,cf_name,key):
	output = keyspace()
	#inp = { 'Key1': {'name':'prajit', 'age':'24'} , 'Key2': {'name':'mayur', 'age':'23'} }
	result = output.colum_family_insert('localhost:9160', ks_name, cf_name, key)
	#result = "Successfully Added"	
	info = { "title" : "Output for Add Entry to Column-Family",
		 "description" : "A simple inquiry of function." }
	return render_template('7_res.html', info = info , result = result )	
#---------------------------------------------------------------------------------------------------------------------------------------			

@app.route('/Drop_Entry_ColumnFamily/', methods=['GET', 'POST'])
def Drop_Entry_ColumnFamily():
	if request.method == 'POST':
		ks_name = request.form["1ks_name"]
		cf_name = request.form["2cf_name"]
		key = request.form["3primary_key"]
    		if ks_name and cf_name and key:
			return Drop_Entry_ColumnFamily_output(ks_name,cf_name,key)
		else:
			return Drop_Entry_ColumnFamily_input("Please enter keyspace, column family & content")
	elif request.method == 'GET':
		return Drop_Entry_ColumnFamily_input()		

def Drop_Entry_ColumnFamily_input(error = ""):
	inputs = { "1ks_name":"Keyspace Name" , "2cf_name":"Column Family Name" , "3primary_key":"Key" }
	info = { "title" : "Drop Entry from Column-Family",
                 "description" : "A simple inquiry of function.",
		 "error" : error }	
	return render_template('8.html', info = info , inputs = inputs )

def Drop_Entry_ColumnFamily_output(ks_name,cf_name,key):
	output = keyspace()
	result = output.column_family_remove('localhost:9160', ks_name, cf_name, key)
	#result = "Successfully Deleted"	
	info = { "title" : "Output for Drop Entry from Column-Family",
		 "description" : "A simple inquiry of function." }
	return render_template('8_res.html', info = info , result = result )		
#---------------------------------------------------------------------------------------------------------------------------------------

@app.route('/Display_Entry_ColumnFamily/', methods=['GET', 'POST'])
def Display_Entry_ColumnFamily():
	if request.method == 'POST':
		ks_name = request.form["1ks_name"]
		cf_name = request.form["2cf_name"]
    		if ks_name and cf_name:
			return Display_Entry_ColumnFamily_output(ks_name,cf_name)
		else:
			return Display_Entry_ColumnFamily_input("Please enter keyspace, column family name")
	elif request.method == 'GET':
		return Display_Entry_ColumnFamily_input()
			
def Display_Entry_ColumnFamily_input(error = ""):
	inputs = {  "1ks_name":"Keyspace Name" ,"2cf_name":"Column Family Name" }
	info = { "title" : "Display Entry from Column-Family",
                 "description" : "A simple inquiry of function.",
		 "error" : error }	
	return render_template('9.html', info = info , inputs = inputs )
	
def Display_Entry_ColumnFamily_output(ks_name,cf_name):
	output = keyspace()
	result = output.colum_family_content('localhost:9160', ks_name, cf_name)
	#print result
	#result = [ ('Key1', OrderedDict([('AGE', '24'), ('NAME', 'PRAJIT')])),('Key2', OrderedDict([('AGE', '23'), ('NAME', 'MAYUR')])) ]
	info = { "title" : "Output for Display Entry from Column-Family",
		 "description" : "A simple inquiry of function." }
	return render_template('9_res.html', info = info , result = result )
#---------------------------------------------------------------------------------------------------------------------------------------

@app.route('/Get_Entry_ColumnFamily/', methods=['GET', 'POST'])
def Get_Entry_ColumnFamily():
	if request.method == 'POST':
		ks_name = request.form["1ks_name"]
		cf_name = request.form["2cf_name"]
		key = request.form["3primary_key"]
    		if ks_name and cf_name and key:
			return Get_Entry_ColumnFamily_output(ks_name,cf_name,key)
		else:
			return Get_Entry_ColumnFamily_input("Please fill all fields")
	elif request.method == 'GET':
		return Get_Entry_ColumnFamily_input()
			
def Get_Entry_ColumnFamily_input(error = ""):
	inputs = { "1ks_name":"Keyspace Name" , "2cf_name":"Column Family Name" , "3primary_key":"Key" }
	info = { "title" : "Get Entry from Column-Family",
                 "description" : "A simple inquiry of function.",
		 "error" : error }	
	return render_template('get_key.html', info = info , inputs = inputs )
	
def Get_Entry_ColumnFamily_output(ks_name,cf_name,key):
	output = keyspace()
	result = output.column_family_get_key('localhost:9160',ks_name,cf_name,key)
	info = { "title" : "Output for Get Entry from Column-Family",
		 "description" : "A simple inquiry of function.", 
		 "key" : key }
	return render_template('get_key_res.html', info = info , result = result )
#---------------------------------------------------------------------------------------------------------------------------------------

@app.route('/Add_Machine/', methods=['GET', 'POST'])
def Add_Machine():
	if request.method == 'POST':
		machine_ip = request.form["2machine_ip"]
		machine_port = request.form["1machine_port"]
		print "out......"
    		if machine_ip and machine_port:
			return Add_Machine_output(machine_ip,machine_port)
		else:
			return Add_Machine_input("Please fill all fields")
	elif request.method == 'GET':
		return Add_Machine_input()
		
def Add_Machine_input(error = ""):
	print "in......"
	inputs = { "2machine_ip":"Machine IP" ,  "1machine_port":"Machine Port"}
	info = { "title" : "Add Machine",
                 "description" : "A simple inquiry of function.",
		 "error" : error }	
	return render_template('mc_add.html', info = info , inputs = inputs )
def Add_Machine_output(machine_ip,machine_port):
	output = keyspace()
	result = output.machine_add(machine_ip,machine_port)
	#print result
	#result = "Successfully Added"	
	info = { "title" : "Output for Add Machine",
		 "description" : "A simple inquiry of function." }
	return render_template('mc_add_res.html', info = info , result = result , error = output.error)
#---------------------------------------------------------------------------------------------------------------------------------------

@app.route('/Delete_Machine/', methods=['GET', 'POST'])
def Delete_Machine():
	if request.method == 'POST':
		machine_ip = request.form["2machine_ip"]
		machine_port = request.form["1machine_port"]
    		if machine_ip and machine_port:
			return Delete_Machine_output(machine_ip,machine_port)
		else:
			return Delete_Machine_input("Please fill all fields")
	elif request.method == 'GET':
		return Delete_Machine_input()
		
def Delete_Machine_input(error = ""):
	inputs = { "2machine_ip":"Machine IP" ,  "1machine_port":"Machine Port"}
	info = { "title" : "Delete Machine",
                 "description" : "A simple inquiry of function.",
		 "error" : error }	
	return render_template('mc_del.html', info = info , inputs = inputs )
def Delete_Machine_output(machine_ip,machine_port):
	output = keyspace()
	result = output.machine_remove(machine_ip,machine_port)
	#print result
	#result = "Successfully Deleted"	
	info = { "title" : "Output for Delete Machine",
		 "description" : "A simple inquiry of function." }
	return render_template('mc_del_res.html', info = info , result = result , error = output.error)
#---------------------------------------------------------------------------------------------------------------------------------------
@app.route('/List_Machines_Cluster/', methods=['GET', 'POST'])
def List_Machines_Cluster(error = ""):
	output = keyspace()
	result = output.machine_get_list()
	#Sresult = ['10.3.3.20:9160','10.3.3.242:9160']	
	info = { "title" : "Output for List machine",
		 "description" : "A simple inquiry of function." }
	return render_template('10_res.html', info = info , result = result)
#---------------------------------------------------------------------------------------------------------------------------------------




class keyspace:
	"""It will contain all keyspace related opertions list,create,delete,select"""
	server_ips = ["10.3.3.20:9160","10.3.3.242:9160"]
	local_system = "localhost:9160"
	error = ""
	complete_list = {}
	
#------------------------------------------- Error testing ---------------------------------------------------------------------
	# Input : machine_id = '10.3.3.20:9160' name = "keyspace_name"
	# Output :  True/False
	def keyspace_contains(self,machine_id,keyspace_name,column_family_name = ''):
		"""Returns true if keyspace with given name and/or column family is on specified machine_id """
		sys = SystemManager(machine_id)
		keyspace.complete_list.clear()
		keyspace_list = sys.list_keyspaces()
		for key in keyspace_list:
			if (key == keyspace_name):
				if(column_family_name):
					column_family_list = sys.get_keyspace_column_families(key, use_dict_for_col_metadata=True)
					for cf in column_family_list:
						if(cf == column_family_name):
							return True
					return False
				else:
					return True
		return False

	# Input : machine_id = '10.3.3.20:9160' name = "keyspace_name"
	# Output :  True/False
	def keyspace_columnfamily_list(self,machine_id):
		"""Returns dictionary of all keyspace with their column family on specified machine_id """
		sys = SystemManager(machine_id)
		keyspace.complete_list.clear()
		keyspace_list = sys.list_keyspaces()
		for key in keyspace_list:
			x=[]
			result = sys.get_keyspace_column_families(key, use_dict_for_col_metadata=True)
			for i in result:
				x.append(i)
			keyspace.complete_list[key] = x
		sys.close()
		return keyspace.complete_list
			
	# Input : machine_ip = '10.3.3.20' ,machine_port = '9160'
	# Output : True/False		the latest updated list
	def machine_add(self,machine_ip,machine_port ='9160'):
		"""Add the machine to list(server_ips) which will be used in pool"""
		entry = machine_ip +":"+ machine_port
		keyspace.server_ips.append(entry)
		return True
	
	# Input : machine_ip = '10.3.3.20' ,machine_port = '9160'
	# Output : True/False		the latest updated list
	def machine_remove(self,machine_ip,machine_port ='9160'):
		"""Remove the machine from list(server_ips) which will be used in pool"""
		entry = machine_ip +":"+ machine_port
		try:
			keyspace.server_ips.remove(entry)
			return True
		except ValueError:
			keyspace.error = "Error : No such key in the list."
			return False
		return True

	def machine_get_list(self):
		return keyspace.server_ips
		
#************************************************** End ************************************************************************

#------------------------------------------- Keyspace----- ---------------------------------------------------------------------
	# Input : machine_id = '10.3.3.20:9160'
	# Output :  ['TK1', 'system', 'testks']	    List of all keyspace on that machine, not possible to pass list of server like in pool 
	def keyspace_get_list(self,machine_id):
		"""Returns all keyspaces in form of list """
		sys = SystemManager(machine_id)
		keyspace_list = sys.list_keyspaces()
		sys.close()
		return keyspace_list

	# Input : machine_id = '10.3.3.20:9160'
	# Output : True
	# Unsuccessful : False
	def keyspace_create(self,machine_id,keyspace_name,replication="1"):
		"""Create keyspace with given name on specified machine_id """
		if (self.keyspace_contains(local_system,keyspace_name) == True):
			print "Error : Keyspace with this name already exist."
			return False
		sys = SystemManager(machine_id)
		print sys.create_keyspace(keyspace_name, SIMPLE_STRATEGY, {'replication_factor': replication})
		self.keyspace_columnfamily_list('localhost:9160')
		sys.close()
		return True
	
	# Input : machine_id = '10.3.3.20:9160' name = "keyspace_name"
	# Output : True
	# Unsuccessful : False
	def keyspace_delete(self,machine_id,keyspace_name):
		"""Delete keyspace with given name on specified machine_id """
		if (self.keyspace_contains(keyspace.local_system,keyspace_name) == False):
			print "Error : Keyspace with this name dosenot exist."
			return False
		sys = SystemManager(machine_id)
		sys.drop_keyspace(keyspace_name)
		self.keyspace_columnfamily_list('localhost:9160')
		sys.close()
		return True
#************************************************** End ************************************************************************

#------------------------------------------- Column Family ---------------------------------------------------------------------
	# Input : machine_id = '10.3.3.20:9160'keyspace_name = 'ks1'
	# Output : ['TestCF','Testcf2']		
	# Unsuccessful : False
	def colum_family_list(self,machine_id,keyspace_name):
		"""List all column family in a given keyspace """
		if (self.keyspace_contains(keyspace.local_system,keyspace_name) == False):
			print "Error : Keyspace with this name dosenot exist."
			return False
		sys = SystemManager(machine_id)
		result = sys.get_keyspace_column_families(keyspace_name, use_dict_for_col_metadata=True)
		x=[]
		for key in result:
			x.append(key)
		return x

	# Input : machine_id = '10.3.3.20:9160',keyspace_name = 'ks1',column_family_name = 'cf1'
	# Output : True
	# Unsuccessful : False
	def colum_family_create(self,machine_id,keyspace_name,column_family_name):
		"""Create a column family in a given keyspace """
		if (self.keyspace_contains(keyspace.local_system,keyspace_name) == False):
			print "Error : Keyspace with this name dosenot exist."
			return False
		sys = SystemManager(machine_id)
		sys.create_column_family(keyspace_name, column_family_name)
		self.keyspace_columnfamily_list('localhost:9160')
		sys.close()
		return True

	# Input : machine_id = '10.3.3.20:9160',keyspace_name = 'ks1',column_family_name = 'cf1'
	# Output : True
	# Unsuccessful : False
	def colum_family_delete(self,machine_id,keyspace_name,column_family_name):
		"""Create a column family in a given keyspace """
		if (self.keyspace_contains(keyspace.local_system,keyspace_name,column_family_name) == False):
			print "Error : Keyspace:column family could not be found."
			return False
		sys = SystemManager(machine_id)
		sys.drop_column_family(keyspace_name, column_family_name)
		self.keyspace_columnfamily_list('localhost:9160')
		sys.close()
		return True
	
	# Important to specify buffer size else cassandra will try to load complete table in main memory which can crash system
	# Input : machine_id = '10.3.3.20:9160',keyspace_name = 'ks1',column_family_name = 'cf1'
	# Output : [ ('Key1', OrderedDict([('AGE', '24'), ('NAME', 'PRAJIT')])),('Key2', OrderedDict([('AGE', '23'), ('NAME', 'MAYUR')])) ]
	# Unsuccessful : False
	def colum_family_content(self,machine_id,keyspace_name,column_family_name):
		"""Returns content of column family of given keyspace """
		if (self.keyspace_contains(keyspace.local_system,keyspace_name,column_family_name) == False):
			print "Error : Keyspace:column family could not be found."
			return False
		pool = ConnectionPool(keyspace = keyspace_name, server_list = keyspace.server_ips, prefill = False)
		col_fam = ColumnFamily(pool, column_family_name)
		tmp_result = col_fam.get_range(start='', finish='',buffer_size=10)
		result = []			
		for i in tmp_result:
			result.append(i)
			#result.update(i)
		#print sum(1 for _ in result)			# for count
		return result

	# Input : machine_id = '10.3.3.20:9160',keyspace_name = 'ks1',column_family_name = 'cf1'
	# Output : True
	# Unsuccessful : False
	def colum_family_insert(self,machine_id,keyspace_name,column_family_name,user_content):
		"""Insert into a column family for a given keyspace """
		if (self.keyspace_contains(keyspace.local_system,keyspace_name,column_family_name) == False):
			print "Error : Keyspace:column family could not be found."
			return False
		pool = ConnectionPool(keyspace = keyspace_name, server_list = keyspace.server_ips, prefill=False)
		col_fam = ColumnFamily(pool, column_family_name)
		#print user_content
		for content in user_content:
			col_fam.insert(content,user_content[content])
		#col_fam.insert('Key2', {'name':'mayur', 'age':'23'})
		return True

	# Input : machine_id = '10.3.3.20:9160',keyspace_name = 'ks1',column_family_name = 'cf1'
	# Output : True
	# Unsuccessful : False
	def column_family_remove(self,machine_id,keyspace_name,column_family_name,key):
		"""Remove a key from column family for a given keyspace """
		if (self.keyspace_contains(keyspace.local_system,keyspace_name,column_family_name) == False):
			print "Error : Keyspace:column family could not be found."
			return False
		pool = ConnectionPool(keyspace = keyspace_name, server_list =  keyspace.server_ips, prefill=False)
		col_fam = ColumnFamily(pool, column_family_name)
		col_fam.remove(key)
		return True

	# Input : machine_id = '10.3.3.20:9160',keyspace_name = 'ks1',column_family_name = 'cf1'
	# Output : True
	# Unsuccessful : False
	def column_family_get_key(self,machine_id,keyspace_name,column_family_name,key):
		"""Remove a key from column family for a given keyspace """
		if (self.keyspace_contains("localhost:9160",keyspace_name,column_family_name) == False):
			print "Error : Keyspace:column family could not be found."
			return False
		pool = ConnectionPool(keyspace = keyspace_name, server_list = keyspace.server_ips, prefill=False)
		col_fam = ColumnFamily(pool, column_family_name)
		print key
		result = col_fam.get(key)
		return result

if __name__ == "__main__":
	app.run(debug=True)
    

