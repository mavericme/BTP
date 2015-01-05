Elandra-Cube
============

Integrating Elasticsearch and Cassandra to Build a custom data cube <br />
Setup:<br />
<pre>
1. Set up cassandra. It holds your data. 
    See http://wiki.apache.org/cassandra/GettingStarted if you need help. 
    You can set up on a multi-computer cluster if you need to.
2. Set up Elasticsearch
    This is required. Visit http://www.elasticsearch.com/ if you need help. 
    You can set up on a multi-computer cluster if you need to.
3. Download Elandra
4. Use Elandra Classes to create Cube.
5. Fire queries!
</pre><br />
Usage Examples:<br />
<pre>
0. You need to set up Schema.json
    It is a json file of the form
    {keyspace1:{table1:{"columns":{column1:details, ....}, ...}, ...}, ...}

    See the given Schema.json for details
    
1. Initial Loading data:
    1.1 You can Load data in cassandra. 
        Configure cassandra.py to match you schema.
        To load data into cube, use ElandraCube.bulkLoad()
    1.2 Alternatively, you can give csv files as input.
        Use csvloader.py to load data to cassandra and build cube.

2. Using the cube - python API
    Use main.py . Re-name it if you like.

    from main import ElandraCube;
    ec_obj=ElandraCube(keyspace="cassandra_keyspace_name", tablename="cassandra_table_name");

    do dir(ElandraCube) to see all functions supported by ElandraCube
    You can use insert(), deleteCube(), query()
    
3. Using the cube- REST api 
    3.1 Run python rest.py on the rest server.
      Make sure you have flask installed first.
    3.2 Create cube
        curl -XPOST http://localhost:5000/makecube?keyspace="cassandra_keyspace_name"&tablename="cassandra_table_name"
       
    3.3 All Usage examples at the end of rest.py
</pre>
<strong style="font-color: red;"> [TODO]</strong> Update readme to demo a complete cube construction
