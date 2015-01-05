import my_code as code
from collections import OrderedDict


# Importing Required Libraries
import pycassa
import bisect
import hashlib
import sys,re
import json

from collections import OrderedDict
from pycassa.system_manager import *
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily	
	
from flask import Flask
from elasticsearch import Elasticsearch
from flask import render_template, request, redirect, url_for

app = Flask(__name__)

@app.route("/test", methods=['GET', 'POST'])
def insert():
	dic1 = {}
	dic2 = OrderedDict();
	dic2['fname'] = '32';
	dic2['lname'] = '42';
	dic1[123] = dic2;
	print dic1
	ks_name = "mykeyspace";
	cf_name = "users1";
	code.test(ks_name,cf_name,dic1);
	l = []
	l = extract();
	print l
	b=[]
	for i in l:
		b.append(int(i['lname']))
	print b
	return b

def extract():
	output=code.keyspace()
	w = output.colum_family_content('localhost:9160',"mykeyspace","users1")
	d={}
	l=[]
	print w
	w.sort(key=lambda x: x[0])
	print w 
	for j in w:
		f={}			
		for k in j[1]:		
			f[str(k)]=str(j[1][k])
		l.append(f)
	return l[0:1]

def main():
	insert();
	extract();

main();
