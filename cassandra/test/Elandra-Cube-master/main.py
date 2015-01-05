#!/usr/bin/env python

#import cassandra.py
#import es.py
import sys,json
import ES,my_code
#import cassandra

import pickle
class ElandraError(Exception):
    #Exception to identify errors in elandra
        def __init__(self, value):
            self.value = value
        def __str__(self):
            return repr(self.value)
#start standard functions
def pout(x):sys.stdout.write(str(x))
def perr(x):sys.stderr.write(str(x))
def dollarEscape(x):
    #x=pickle.dumps(x)
    x=str(x)
    ret=''
    l=len(x)
    for i in xrange(0,l,1):
	    if(x[i:i+1]=='\\'):
		    ret+='\\\\'
	    elif(x[i:i+1]=='$'):
		    ret+='\\$'
	    else: ret+=x[i:i+1]
    return ret
def dollarUnescape(x):
#	x.replace("\\$","$").replace("\\\\","\\")
    ret='';l=len(x);i=0
    while(i<l):
	    if(i<l-1 and x[i:i+2]=='\\$'):
		    ret+='$';i+=1;
	    elif (i<l-1 and x[i:i+2]=='\\\\'):
		    ret+='\\';i+=1;
	    else:
		    ret+=x[i:i+1];
	    i+=1
    return ret #pickle.loads(ret) #first unescape then load
def dollarSeparate(x):
    #has escaped and unescaped dollars
    l=len(x)
    i=0;prev=0;ret=[]
    while(i<l):
        if  x[i:i+1]=="$":
            ret.append(x[prev:i])
            prev=i+1
        elif i<l-1:
            if x[i:i+2]=='\\\\' or x[i:i+2]=='\\$':
                i+=1;        
        i+=1
    ret.append(x[prev:i])
    return ret
def dollarAdd(x,y):
    #add y to x
    return x+"$"+dollarEscape(y)

#end standard functions
class ElandraCube:

    def __init__(self,keyspace=sys.argv[1],tablename=sys.argv[2],cassandra_host="localhost:9160",elasticsearch_seeds=['localhost:9200'],cube_details=None):
	    #cube_details is list of columns used to create index, in order or cube creation
        self.initStatus=0;self.ncols=0;self.ex_obj=None;self.schema=None;self.cass_obj=None;#self.config=[]
	self.keyspace=None;self.tablename=None;self.aggregateCols=None;self.esIndexName=None;
	#start input vars
        self.list_seeds=elasticsearch_seeds #for elasticsearch
	self.cassandra_host=cassandra_host #for cassandra
        self.keyspace=keyspace
	self.tablename=tablename
	self.esIndexName=dollarAdd(dollarEscape(keyspace),tablename)
        #end input variables
	self.init(cube_details)

    def getSchema(self):
	    #internal function
        try:
		fp=open("schema.json","r")
	except IOError:
		raise ElandraError("Could not find Schema file schema.json")
	try:
		self.schema=json.loads(fp.read())
	except ValueError:
		raise ElandraError("Schema may be invalid JSON")
	fp.close()

    def init(self,details):
	    #called automatically at creation
        self.es_obj=ES.ESObj()
        if not self.es_obj.connect(self.list_seeds): raise ElandraError("ElasticSearch Connection Failed");
        #read schema
	self.getSchema()
	if details:
		self.makeCube(details)
	else:
		#perr("index columns list not supplied at creation, automatically selecting columns")
		self.makeCube(self.getAutomaticallyIndexColumns())
        perr("init successful, connected to elasticsearch %s\n"%(self.list_seeds))

  
    def makeCube(self,details):
        #details=[col1, col2, ... cube column list]
        #cube column list (in order of sequence of cube creation -> status
        #gen combination columns
	self.genColumns(details)              #create columns list for getting order later, or using default order
        self.updateAggregates()
	#perr("makeCube successful")
    def getAutomaticallyIndexColumns(self):
	    schema=self.schema
	    columns=self.schema[self.keyspace][self.tablename]["columns"]
	    ret=[]
	    for col in columns:
		    if(columns[col].has_key("index_type")):
			       if(columns[col]["index_type"]=="index"):
				       ret.append(col)
		    else: raise ElandraError("Column %s has no index_type"%(col))
	    return ret
    def updateAggregates(self):
	    #internal aggregate columns list updation
	    self.aggregateCols=[]
	    columns=self.schema[self.keyspace][self.tablename]["columns"]
	    for col in columns:
		    if(columns[col].has_key("index_type")):
			    if(columns[col]["index_type"]=="aggregate"):
				    self.aggregateCols.append(col)
		    else:
			    raise ElandraError("Column %s has no index_type"%(col))
	    
    def genColumns(self,details):
	    # cube column list (in order of sequence of cube creation -> status
	    # order of columns to materialize
	    # called by makeCube
	    # updates self.indexCols (useless) and self.orderedCols (useful to get storage order)
           
        self.cols=None
        # if(len(details.keys())!=1 or len(details[details.keys()[0]])!=1 or len(details[details.keys()[0]][details[details.keys()[0]].keys()[0]])!=1):
        #         raise ElandraError("Malformed expression, require details={keyspace:{tableName:{\"columns\":[col1, col2, ... optional cube column list]}}}")
        keyspace=self.keyspace
        tablename=self.tablename
	#print "adding dollarEscaped %s %s"%(keyspace,tablename)
	incols=details
        if(incols is None):
                #perr("No columns")
                cols=sorted(self.schema[keyspace][tablename]["columns"].keys())
                for col in cols:
                        if(cols[col]["index_type"]=="index"):
                                incols.append(col)
        nc=len(incols)
        cols=self.schema[keyspace][tablename]["columns"]
	for i in incols:
            if(cols.has_key(i) and cols[i]["index_type"]=="index"):pass
            else: raise ElandraError("Invalid column in Column")
	
        self.indexCols=[]
        self.orderedCols=incols              #set order of columns to make multidimension search
        for i in xrange(1,(1<<nc),1):
                curcol=''
                conf=bin(i)[2:];lconf=len(conf);conf='0'*(nc-lconf)+conf
                #perr(conf)
                for j,k in enumerate(conf):
                        if(k=='1'):curcol=dollarAdd(curcol,incols[j])
                if(curcol):curcol=curcol[1:]
                self.indexCols.append(curcol)
        #print incols
        #print self.indexCols

    def query(self,vals):
        #vals(dictionary)->query result(json) or False
        #{"col1":val2,"col2":val2} dictionary
        qcols={}
        qtablename=''
        keyspace=self.keyspace
        tablename=self.tablename
        qtablename=dollarAdd(dollarEscape(keyspace),tablename)
        qcols=vals
        qcolname=''
        qval=''
        cols=self.schema[keyspace][tablename]["columns"]
        for colname in cols:
            if(cols[colname].has_key("index_type") ):
                if cols[colname]["index_type"]=="index":
                    if(qcols.has_key(colname)):
                        qcolname=dollarAdd(qcolname,colname)
                        qval=dollarAdd(qval,(qcols[colname])) # todo value needs to be serialized before entering index
		#else: print "ignoring",colname, "with type", cols[colname]["index_type"]
            else: raise ElandraError("column %s has no type"%(colname))
        if qcolname: 
            qcolname=qcolname[1:]#remove extra dollar
            qval=qval[1:]
        else:
            qcolname="$" #global aggregate
            qval="$"
        return self.es_obj.getIndex(self.esIndexName,qcolname,qval)

    def insert(self,valsWithoutDatatype):
        #{"c1":val1,"c2":val2} vals in no particular order
	    #returns True on success, throws ElandraError on failure
        try:
		qtablename=dollarAdd(dollarEscape(self.keyspace),self.tablename)
	except:
		raise ElandraError("Error: Object may not be initialized")
	orderedCols=self.orderedCols 
	nc=len(orderedCols)
	#datatype conversion TODO remove this
	iv={}
	for key in valsWithoutDatatype:
		dt=self.schema[self.keyspace][self.tablename]["columns"][key]["type"]
		try:
			iv[key]=__builtins__[dt](valsWithoutDatatype[key])
		except:
			iv[key]=__builtins__.__dict__[dt](valsWithoutDatatype[key])
	vals=iv#self.insert(iv)
	#vals=valsWithoutDatatype
        #done datatype conversion
	for i in xrange(0,(1<<nc),1): #loop to get 2**c indexes
                curcolname=''
		curval=''
                conf=bin(i)[2:];lconf=len(conf);conf='0'*(nc-lconf)+conf
                #perr(conf)
                for j,k in enumerate(conf):
                        if(k=='1'):
				tempcolname=orderedCols[j]
				curcolname=dollarAdd(curcolname,tempcolname)
				curval=dollarAdd(curval,(vals[tempcolname] if vals.has_key(tempcolname) else ''))
		if curcolname:
			curcolname=curcolname[1:]
			curval=curval[1:]
		else:
			curcolname='$'
			curval='$'
		#have one col and val
		es_index=self.es_obj.getIndex(self.esIndexName,curcolname,curval)
		data=None
		if not es_index:
			#create index
			data={"aggregates":{}}
			data["value"]=curval
			for aggf in self.aggregateCols:
				aggv=None
				if(vals.has_key(aggf)):
					aggv=vals[aggf]
				data["aggregates"][aggf]={"min":aggv,"max":aggv,"sum":aggv,"count":1}
		else:
			data=es_index["_source"]
			for aggf in self.aggregateCols:
				aggv=None
				if(vals.has_key(aggf)):
					aggv=vals[aggf]
					if aggv is None: continue
					da=data["aggregates"][aggf];oldmin=da["min"];oldmax=da["max"];oldsum=da["sum"];oldcount=da["count"]
					data["aggregates"][aggf]={"min":(aggv if oldmin is None else min(aggv,oldmin) if aggv is not None else oldmin),\
								  "max":(aggv if oldmax is None else max(oldmax,aggv) if aggv is not None else oldmax),\
								  "sum":(aggv if oldsum is None else oldsum+aggv if aggv is not None else oldsum),\
								  "count":oldcount+1}
			

	        #update index
		if not self.es_obj.createIndex(self.esIndexName,curcolname,curval,data):
			raise ElandraError("Could not Insert!")
	return True
#for i in range(self.ncols):
            
    def deleteCube(self):
	    # delete all data in current cube
	    # returns True on success, throws error on failure
	    if not self.es_obj.deleteIndex(self.esIndexName):
		    raise ElandraError("Could not delete Cube %s"%(str(dollarSeparate(self.esIndexName))))
	    perr("Cube succesfully deleted")
	    return True
    def bulkLoad(self,cassandra_host=None):
	    if cassandra_host==None:
		    cassandra_host=self.cassandra_host
	    self.cass_obj=cassandra.cassandra_connection(keyspace=self.keyspace, table=self.tablename,host=cassandra_host)
	    if not self.cass_obj:raise ElandraError("Cassandra connection Failed")

	    for item in self.cass_obj.get_iterator():
		    iv={}
		    for key in item[1]:
			    dt=self.schema[self.keyspace][self.tablename]["columns"][key]["type"]
			    try:
				    iv[key]=__builtins__[dt](item[1][key])
			    except:
				    iv[key]=__builtins__.__dict__[dt](item[1][key])
		    self.insert(iv)
ecobj=None
def test():# if __name__=="__main__":
	perr("Begin Testing\n")
	global ecobj
	ecobj=ElandraCube(# cube_details=["lname","fname"]
		)
	#ecobj.makeCube()
	#ecobj.deleteCube()
	output = my_code.keyspace()
	w = output.colum_family_content('localhost:9160', sys.argv[1],sys.argv[2])
	l=[]
	for j in w:
		f={}			
		for k in j[1]:
			q=k
			f[q]=j[1][k]
		l.append(f)
	for i in l:
		ecobj.insert(i)
#	ecobj.insert({"fname":"abcd","lname":"pqr"})
#	ecobj.insert({"fname":"abcde","lname":"ppqr","marks":20})
	print ecobj.query({"lname":"pqr"})
	perr("Tests Succesful\n")

test()
