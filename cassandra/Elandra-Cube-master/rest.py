#!/usr/bin/env python
from flask import Flask, jsonify
import flask,json
from main import dollarEscape, dollarUnescape, dollarSeparate, dollarAdd,perr,pout, ElandraCube
import httplib,re #for init

#globals
app=Flask(__name__)
cubes={}
init_done=False

@app.route("/cubes",methods=['GET','PUT','POST'])
def handleGetCubeNames():
    return json.dumps(cubes.keys())
@app.route("/",methods=['GET'])
def handleDefault():
    if not init_done: return init()
    #perr(cubes)
    return jsonify({"status":"ok"})

# /makecube/?keyspace=k1&tablename=t1
@app.route("/makecube",methods=['PUT','POST'])
def handleMakeCube():
    global cubes
    keyspace=flask.request.args.get('keyspace','')
    table=flask.request.args.get('tablename','')
    if not table or not keyspace:
        perr("Invalid request")
        return jsonify({"status":"failed","error":"No tablename or keyspace"})
    newcubename=(dollarAdd(dollarEscape(keyspace),table))
    if cubes.has_key(newcubename):
        return jsonify({"status":"failed","error":"Cube already exists"})
    cubes[newcubename]=ElandraCube(keyspace=keyspace, tablename=table)
    return jsonify({"status":"success"})

def init():
    global cubes,init_done
    cubes={}
    conn = httplib.HTTPConnection("localhost:9200") #TODO connections to remote setups
    conn.request('GET', '/_status', '')
    resp = conn.getresponse()
    content = json.loads(resp.read())
    for indexdetail in content["indices"]: #TODO check out if better search method exists 
        tname=indexdetail
        fresult=re.findall("[^$]*\$[^$]*",tname)
        if fresult and (tname in fresult):
            params=dollarSeparate(tname)
            try:
                cubes[tname]=ElandraCube(keyspace=params[0],tablename=params[1])
                perr( "loaded cube "+unicode( tname));perr('\n')
            except:
                perr( "cube creation failed for "+unicode( tname));perr('\n')
    init_done=True
    print cubes
    return {"status":"ok"}


# /insert/?keyspace=k1&tablename=t1 -d{"c1":val1,"c2":val2",..}
@app.route("/insert",methods=['POST','PUT'])
def handleInsert():
    keyspace=flask.request.args.get('keyspace','')
    table=flask.request.args.get('tablename','')
    data=json.loads(flask.request.get_data(as_text=True))
    if not table or not keyspace:
        perr("Invalid request")
        return jsonify({"status":"failed","error":"No tablename or keyspace"})
    cubename=(dollarAdd(dollarEscape(keyspace),table))
    if(cubes.has_key(cubename)): 
        ecobj=cubes[cubename]
        if ecobj.insert(data):
            return jsonify({"status":"ok"})
        return jsonify({"status":"failed","error":"Unknown error in ElandraCube insert"})
    else: return jsonify({"status":"failed","error":"Cube does not exist"})



# /query/?keyspace=k1&tablename=t1 -d{"c1":val1,"c2":val2",..}
@app.route('/query',methods=['POST','PUT'])
def query():

    keyspace=flask.request.args.get('keyspace','')
    table=flask.request.args.get('tablename','')
    data=json.loads(flask.request.get_data(as_text=True))
    if not table or not keyspace:
        perr("Invalid request")
        return jsonify({"status":"failed","error":"No tablename or keyspace"})
    cubename=(dollarAdd(dollarEscape(keyspace),table))
    if(cubes.has_key(cubename)): 
        ecobj=cubes[cubename]
        queryret=ecobj.query(data);
        if queryret:
            return (json.dumps(queryret))
        return jsonify({"status":"failed","error":"Your Search %s did not match any index"%(json.dumps(data))})
    else: return jsonify({"status":"failed","error":"Cube does not exist"})


init()
app.run(debug=True )

#####notes########
# rest api does not support custom column order, default order is used . ignore this sentence

### usage guide ###
# insert
# curl -XPUT <url>/insert/?keyspace=k1&tablename=t1 -d{"c1":val1,"c2":val2",..}
# makecube
# curl -XPUT /makecube/?keyspace=k1&tablename=t1
# query
# specify columns for only restricted ones, rest are unbounded
# curl -XPUT /query/?keyspace=k1&tablename=t1 -d{"c1":val1,"c2":val2",..}
