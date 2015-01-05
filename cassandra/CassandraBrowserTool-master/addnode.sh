#!/bin/sh

# Firstly to install the packages on the machine
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

# Running cassandra server
cassandra -f # run in backend

echo
echo Cassandra Running 
echo
echo


# Check whether Cassandra server is running or not on the added node
check_process(){
        # check the args
        if [ "$1" = "cassandra" ];
        then
                return 0
        fi

        PROCESS_NUM='ps -ef | grep "$1" | grep -v "grep"'
        $PROCESS_NUM
        if [ $PROCESS_NUM -eq 1 ];
        then
                return 1
        else
                return 0
        fi
}


# to open the file cassandra.yaml on the added node
filename='cassandra.yaml'   # Specify the complete path of cassandra.yaml file


# to make the desired changes in the cassandra.yaml file
while read line
do
string=`echo $line`
IFS=':' read -a array <<< "$string"
first=`echo "${array[0]}"`
cluster=`echo $line`
echo $cluster
sed -i "/$first/c\\$cluster" $filename
done < input


# to run another script to generate hashes  # this has to be checked up



