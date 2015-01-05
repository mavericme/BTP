cd ~/ubuntu
sudo apt-get update
sudo apt-get install openjdk-7-jre-headless -y
 
### Check http://www.elasticsearch.org/download/ for latest version of ElasticSearch and replace wget link below
 
# NEW WAY / EASY WAY
wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.1.0.deb
sudo dpkg -i elasticsearch-1.1.0.deb
 
# OLD WAY / HARD WAY
wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.1.0.tar.gz
tar -xf elasticsearch-1.1.0.tar.gz
rm elasticsearch-1.1.0.tar.gz
sudo mv elasticsearch-* elasticsearch
sudo mv elasticsearch /usr/local/share
 
curl -L http://github.com/elasticsearch/elasticsearch-servicewrapper/tarball/master | tar -xz
sudo mv *servicewrapper*/service /usr/local/share/elasticsearch/bin/
rm -Rf *servicewrapper*
sudo /usr/local/share/elasticsearch/bin/service/elasticsearch install
sudo ln -s `readlink -f /usr/local/share/elasticsearch/bin/service/elasticsearch` /usr/local/bin/rcelasticsearch
 
sudo service elasticsearch start
#curl http://localhost:9200
