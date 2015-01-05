import flask,sys,os,json
schema=json.loads('''{
    "k1": {
        "t1": {
            "columns": { 
                "id": { "type": "int", "primary_key": true, "index_type": "ignore" },
                "fname": { "type": "text", "primary_key": true, "index_type": "index" },
                "lname": { "type": "text", "primary_key": false, "index_type": "index" },
                "marks": { "type": "int", "primary_key": false, "index_type": "aggregate" }
            }
        },

        "t2": {
            "columns": { 
								"id": { "type": "int", "primary_key": true, "index_type": "ignore" },
             		"region": { "type": "text", "primary_key": true, "index_type": "index" },
                "year": { "type": "text", "primary_key": false, "index_type": "index" },
                "sales": { "type": "int", "primary_key": false, "index_type": "aggregate" }
            }
        }
    },
    "k2": {
        "t3": {
            "columns": {}
        }
    }
}''')
class b:
    def __init__(self,x):
        self.schema=x
vals={'k1':{'t1':{'fname':"john",'lname':'doe'}}}
a=b(schema)
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
    return x.replace("\\","\\\\").replace("$","\\$")
def dollarUnescape(x):
    return x.replace("\\$","$").replace("\\\\","\\")
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
def query(self,vals):
    #vals(dictionary)->status (bool)
    #{"keyspace":{"tablename":{"col1":val2,"col2":val2}}} dictionary
    if(len(vals)!=1):raise ElandraError("Need one Keyspace in handleRow()")
    qcols={}
    qtablename=''
    keyspace=vals.keys()[0]
    if len(vals[keyspace])!=1 : raise ElandraError("Need one Table in the one Keyspace in handleRow(), which has all accumulated data")
    tablename=vals[keyspace].keys()[0]
    qtablename=dollarAdd(dollarEscape(keyspace),tablename)
    qcols=vals[keyspace][tablename]
    qcolname=''
    qval=''
    cols=self.schema[keyspace][tablename]
    for colname in cols:
        if(cols[colname].has_key("type") ):
            if cols[colname]["type"]=="index":
                if(qcols.has_key(colname)):
                    qcolname=dollarAppend(qcolname,colname)
                    qval=dollarAppend(qval,qcols[colname])
        else: raise ElandraError("column %s has no type"%(colname))
    if qcolname: 
        qcolname=qcolname[1:]#remove extra dollar
        qval=qval[1:]
    else:
        qcolname="$" #global aggregate
        qval="$"
    print qtablename,qcolname,qval
    #TODO add global aggregate everywhere
    return es_obj.getIndex(qtablename,qcolname,qval)
