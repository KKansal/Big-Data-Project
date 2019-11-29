import load
import parser
import os

while 1:
	print("-->",end=" ")
	query  = input();
	if(query=="exit"):
		break;
	queries = parser.parser(query)
	# print(queries)
	db_name = queries['data']
	table_name = queries['table']
	list_queries = list(queries.keys())


	print(list_queries)
	if(len(list_queries)==1):
		 if('load' in list_queries):
		 		new_proc = os.fork()
		 		if(new_proc == 0):
		 			# print(queries['load'])
		 			db_name,table_name,schema=queries['load'].split()
		 			load.load_data(db_name,table_name,schema)
		 		else:
		 			os.wait()


		 elif('delete' in list_queries):
		 		new_proc = os.fork()
		 		if(new_proc == 0):
		 			os.system("python3 hdfs-del-Webhdfs.py " + queries['delete'])
		 		else:
		 			os.wait()

		 elif('project' in list_queries):
		 		new_proc = os.fork()
		 		if(new_proc == 0):
		 			os.system('hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-*streaming*.jar\
		 						-D mapred.reduce.tasks=0\
		 						-files "hdfs://localhost:9000/sql/' + db_name + '/' + table_name + '/schema.txt"' + \
		 						'-file select_mapper.py \
		 						-mapper "python3 select_mapper.py ' + queries['select'] + \
		 						'-input /sql/' + db_name + '/' + table_name + '/' + table_name + '.csv' + \
		 						'-output /sql/final_out')
		 		else:
		 			os.wait()
	else:
		if(('select' in list_queries) and ('aggregate' in list_queries)):   #SELECT COL1 FROM DB/T.CSV WHERE COL1=VAL1 AGGREGATE BY COUNT;"
		 		new_proc = os.fork()
		 		if(new_proc == 0):
		 			os.system('hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-*streaming*.jar ' + \
		 						'-files "hdfs://localhost:9000/sql/' + db_name + '/' + table_name + '/schema.txt" ' + \
		 						'-file select_mapper.py ' + \
		 						'-mapper "python3 select_mapper.py ' + queries['select'] + '\" ' + \
		 						'-file agg_reducer.py ' + \
		 						'-reducer "python3 agg_reducer.py'   + queries['aggregate'] + '\" ' + \
		 						'-input /sql/' + db_name + '/' + table_name + '/' + table_name + '.csv ' + \
		 						'-output /sql/final_out')
		 		else:
		 			os.wait()
		elif(('select' in list_queries) and ('project' in list_queries)):  #SELECT COL1 FROM DB/T.CSV WHERE COL1=VAL1;
		 		new_proc = os.fork()
		 		if(new_proc == 0):
		 			os.system('hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-*streaming*.jar ' \
		 						'-D mapred.reduce.tasks=0 ' + \
		 						'-files "hdfs://localhost:9000/sql/' + db_name + '/' + table_name + '/schema.txt" ' + \
		 						'-file select_mapper.py' + \
		 						'-mapper "python3 select_mapper.py ' + queries['select'] + '\" ' + \
		 						'-input /sql/' + db_name + '/' + table_name + '/' + table_name + '.csv ' + \
		 						'-output /sql/temp/select_out')

		 			os.system('hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-*streaming*.jar ' + \
		 						'-files "hdfs://localhost:9000/sql/' + db_name + '/' + table_name + '/schema.txt" ' + \
		 						'-file project_mapper.py ' + \
		 						'-mapper "python3 project_mapper.py ' + queries['project'] + '\" '+ \
		 						'-file remdup_reducer.py '+ \
		 						'-reducer "python3 remdup_reducer.py" ' + \
		 						'-input /sql/temp/select_out ' + \
		 						'-output /sql/final_out')
		 		else:
		 			os.wait()
		elif(('project' in list_queries) and ('aggregate' in list_queries)):
		 		if(new_proc == 0):
		 			os.system('hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-*streaming*.jar ' + \
		 						'-files "hdfs://localhost:9000/sql/' + db_name + '/' + table_name + '/schema.txt" ' + \
		 						'-file identity_mapper.py ' + \
		 						'-mapper "python3 identity_mapper.py ' + queries['select'] + '\" ' + \
		 						'-file agg_reducer.py ' + \
		 						'-reducer "python3 agg_reducer.py'   + queries['aggregate'] + '\" ' + \
		 						'-input /sql/' + db_name + '/' + table_name + '/' + table_name + '.csv ' + \
		 						'-output /sql/final_out')
		 		else:
		 			os.wait()
		 		
