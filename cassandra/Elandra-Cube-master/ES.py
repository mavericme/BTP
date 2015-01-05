import elasticsearch
import httplib 
## [list-of seeds] -> object-of elasticsearch

"""
	Index mapping of cassandra in elasticsearch

	host:9200    <- This will be host
	index_name  <- will be the table name
	index_type  <- column combination of the format column1_name$column2_name$... columnN_name$ make sure to add $ after every column_name
	index_id    <- this is of the format of index_type just the column_names are replaced by values
	body        <- format: dictionary of the form:
				{ aggregation_type1 : {aggregation_column1:value1 ...} ... }

"""
		      	  
class ESObj:
	es=None
	list_seeds=None
	def connect(self,list_seeds):
		"""
			creates a connection object which you need to specify in all your further queries

			@args
			list_of_seeds (can be any node in elasticsearch) in following format
			["192.168.1.3:9200","192.168.1.4:9200"]

			@ret
			if successful then elasticsearch object
			else False
		"""
		try:
		   self.es = elasticsearch.Elasticsearch(list_seeds,sniff_on_start=True)
		   self.list_seeds=list_seeds
		   return True
		except:
		   return False

	def createIndex(self,document_index,document_type,document_id,document_body):
		es_obj=self.es
		"""
			creates the corresponding index in the es_obj (1st parameter)  with following specifications:
				index: document_index (2nd parameter)
				type: document_type (3rd parameter)
				id: document_id (4th parameter)
				body: document_body (5th parameter)

			@args: elasticsearch-object? string? string? string? dictionary?

			@ret: bool?
		"""
		try:
			p=es_obj.index(
				index=document_index,
				doc_type=document_type,
				id=document_id,
				body=document_body
			    )
			return True;
		except:
			return False;

	def getIndex(self,document_index,document_type,document_id):
		es_obj=self.es
		"""
			gets the index as per specification

			@args: elasticsearch-object? string? string? string?

			@ret: dictionary?

		"""
		try:
			return es_obj.get(index=document_index,doc_type=document_type,id=document_id)
		except:
			return False
	def deleteIndex(self,document_index):
		try:
			
			conn = httplib.HTTPConnection(self.list_seeds[0])
			conn.request('DELETE', '/'+document_index, '') 
			resp = conn.getresponse()
			content = resp.read()
			print content
			return True
		# return self.es.delete(index=document_index)
	
		except:
			return False
