#invoke the python script as python csvloader.py schema.json keyspace table csv_file
#schema.json - contains the schema of the databases ### may contain multiple keyspaces and tables
#
#
#

import sys
import os
import collections
import json
import pycassa
import csv
import ast
from cassandra import cassandra_connection
from main import ElandraCube

if len(sys.argv) < 5:
	print "Not enough arguments."
	exit(1)

#try:
#	pool = pycassa.ConnectionPool(sys.argv[2])
#except:
#	print "The keyspace doesn't exist. Exiting now..."
#	exit(1)
#
#try:
#	table=pycassa.ColumnFamily(pool,sys.argv[3])
#except:
#	print "The table doesn't exist. Exiting now..."
#	exit(1)
#
records_inserted=0
if os.path.isfile('.records_inserted')==False:
	temp_file=open('.records_inserted','w')
	temp_file.write('0\n');
	temp_file.close();

temp_file=open('.records_inserted','r')
records_inserted=int(temp_file.readline().split('\n')[0])
temp_file.close()

#command = "echo \" copy " + sys.argv[2]+"."+sys.argv[3] + " from '" + sys.argv[4] +"';\" | cqlsh"
#print command

#os.system(command)


#print "Data loaded into Cassandra successfully. Now calling ElandraCube object..."

json_file=open(sys.argv[1])
#print json_file
#the second argument in the following function is to maintain the order of json file	
json_data=json.load(json_file,object_pairs_hook=collections.OrderedDict)
#print json_data[str(sys.argv[2])]
column_names=json_data[sys.argv[2]][sys.argv[3]]['columns']
#print column_names

obj = ElandraCube(keyspace=sys.argv[2],tablename=sys.argv[3])
cc  = cassandra_connection(str(sys.argv[2]),str(sys.argv[3]))

with open(sys.argv[4],'rb') as csvfile:
	f=csv.reader(csvfile,delimiter=",")
	for row in f:
#print row
		query="{"
		j=0
		for i in column_names:
			if j!=0:
				query+=","
			query+='"'+i+'"'+':"'+row[j]+'"'
			j+=1
		query+="}"
#	print query
#	print ast.literal_eval(query)
		obj.insert(ast.literal_eval(query))
		cc.insert(str(records_inserted),ast.literal_eval(query))
		records_inserted+=1		
		print records_inserted
temp_file=open('.records_inserted','w')
temp_file.write(str(records_inserted)+'\n')
temp_file.close()
