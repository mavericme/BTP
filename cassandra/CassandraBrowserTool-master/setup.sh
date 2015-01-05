#!/bin/bash

# script for installing and running our Source Code
# Assuming the user is loginned as Super-User and if not login then it will ask for sudo access

# Installing required libraries :: pycassa, flask

apt-get install python-pycassa

for i in python-pycassa python-flask; do
  sudo apt-get install $i
done


echo
echo Pycassa API and Flask API Installed
echo
echo

# Installing cassandra server

sudo apt-get install cassandra

echo
echo Cassandra Server Installed
echo
echo

# Running cassandra server in backend

nohup cassandra -f >>/dev/null 2>>/dev/null &

echo
echo Cassandra Running 
echo
echo


# Running our Python Backend Code

python Cassandra/my_code.py


# Pointing to the Localhost IP of the Machine on which the setup file is running

firefox http://127.0.0.1:5000/




