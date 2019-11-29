

while 1:
	print("-->",end=" ")
	query  = input();
	if(query=="exit"):
		break;
	queries = parser(query)
	
	list_queries = queries.keys()

	if(len(list_queries)==1):
		 if('load' in list_queries):
		 		new_proc = os.fork()
		 		if(new_proc == 0):
		 			os.system("python3 hdfs-load-Webhdfs.py " + queries['load'])
		 		else:
		 			os.wait()

		 elif('delete' in list_queries):
		 		new_proc = os.fork()
		 		if(new_proc == 0):
		 			os.system("python3 hdfs-del-Webhdfs.py " + queries['delete'])
		 		else:
		 			os.wait()

		 elif('select' in list_queries):
		 		new_proc = os.fork()
		 		if(new_proc == 0):
		 			os.system('hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-*streaming*.jar\
		 						-D mapred.reduce.tasks=0\
		 						-files "hdfs://localhost:9000/sql/' + db_name + '/' + table_name + '/schema.txt"' + \
		 						'-file select_mapper.py \
		 						-mapper "python3 select_mapper.py ' + queries['select'] + \
		 						'-input /sql/' + db_name + '/' + table_name + '/' + table_name + '.csv' + \
		 						'-output /sql/finalout')
		 		else:
		 			os.wait()
	else:
		if('select' in list_queries) and ('aggregate' in list_queries):
		 		new_proc = os.fork()
		 		if(new_proc == 0):
		 			os.system('hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-*streaming*.jar' \
		 						'-files "hdfs://localhost:9000/sql/' + db_name + '/' + table_name + '/schema.txt"' + \
		 						'-file select_mapper.py' \
		 						'-mapper "python3 select_mapper.py ' + queries['select'] + '"' + \
		 						'-file agg_reducer.py'
		 						'-reducer "python3 agg_reducer.py'   + queries['aggregate']
		 						'-input /sql/' + db_name + '/' + table_name + '/' + table_name + '.csv' + \
		 						'-output /sql/temp/select_out')
		 		else:
		 			os.wait()
		if('aggregate' in list_queries):
		 		new_proc = os.fork()
		 		if(new_proc == 0):
		 			os.system('hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-*streaming*.jar\
		 						-D mapred.reduce.tasks=0\
		 						-files "hdfs://localhost:9000/sql/' + db_name + '/' + table_name + '/schema.txt"' + \
		 						'-file select_mapper.py \
		 						-mapper "python3 select_mapper.py ' + queries['select'] + \
		 						'-input /sql/' + db_name + '/' + table_name + '/' + table_name + '.csv' + \
		 						'-output /sql/temp/select_out')
		 		else:
		 			os.wait()
	



