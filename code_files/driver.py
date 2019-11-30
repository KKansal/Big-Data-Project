import load
import parser
import delete
import read
import os

while 1:
	
	print("-->",end=" ")
	query  = input();
	if(query=="exit"):
		exit()
	queries = parser.parser(query)
	if(queries==-1):
		continue
	# print(queries)
	# print(queries)
	try:
		db_name = queries['database']
		table_name = queries['table'].rstrip(".csv")
		del queries['database']
		del queries['table']
	except:
		pass
	list_queries = list(queries.keys())
	# print(db_name,table_name,list_queries)

	# print(list_queries)
	if(len(list_queries)==1):
		 if('load' in list_queries):
	 			# print(queries['load'])
	 			db_name,table_name,schema=queries['load'].split()
	 			load.load_data(db_name,table_name,schema)
	 			continue

		 elif('delete' in list_queries):
	 			db_name,table_name=queries['delete'].split()
	 			table_name = table_name.rstrip(".csv")
	 			res = delete.delete_folder(db_name + "/")
	 			if res == True:
	 				print("Database deleted successfully")
	 			else:
	 				print("Given Database doesnot exist")
	 			continue
		 		
		 		
		 elif('project' in list_queries):
		 		new_proc = os.fork()
		 		if(new_proc == 0):
		 			cmd = 'hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-*streaming*.jar ' + \
		 						'-D mapred.reduce.tasks=0 ' + \
		 						'-files "hdfs://localhost:9000/sql/' + db_name + '/' + table_name + '/schema.txt" ' + \
		 						'-file project_mapper.py '+ \
		 						'-mapper "python3 project_mapper.py ' + queries['project'] + '" '+ \
		 						'-input /sql/' + db_name + '/' + table_name + '/' + table_name + '.csv ' + \
		 						'-output /sql/final_out'

		 			# print(cmd)
		 			os.system(cmd)
		 			exit()
		 		else:
		 			os.wait()
	else:
		if(('select' in list_queries) and ('aggregate' in list_queries)):   #SELECT COL1 FROM DB/T.CSV WHERE COL1=VAL1 AGGREGATE BY COUNT;"
		 		new_proc = os.fork()
		 		if(new_proc == 0):
		 			# print(db_name,table_name)
		 			cmd = 'hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-*streaming*.jar ' + \
		 						'-files "hdfs://localhost:9000/sql/' + db_name + '/' + table_name + '/schema.txt" ' + \
		 						'-file select_mapper.py ' + \
		 						'-mapper "python3 select_mapper.py ' + queries['select'] + '\" ' + \
		 						'-file agg_reducer.py ' + \
		 						'-reducer "python3 agg_reducer.py'   + " " + queries['aggregate'][0] + "," +queries['aggregate'][1] + '\" ' + \
		 						'-input /sql/' + str(db_name) + '/' + str(table_name) + '/' + str(table_name) + '.csv ' + \
		 						'-output /sql/final_out'
		 			# print(cmd)
		 			os.system(cmd)
		 			exit()
		 		else:
		 			os.wait()
		elif(('select' in list_queries) and ('project' in list_queries)):  #SELECT COL1 FROM DB/T.CSV WHERE COL1=VAL1;
		 		new_proc = os.fork()
		 		if(new_proc == 0):
		 			cmd = 'hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-*streaming*.jar ' \
		 						'-D mapred.reduce.tasks=0 ' + \
		 						'-files "hdfs://localhost:9000/sql/' + db_name + '/' + table_name + '/schema.txt" ' + \
		 						'-file select_mapper.py ' + \
		 						'-mapper "python3 select_mapper.py ' + queries['select'] + '\" ' + \
		 						'-input /sql/' + db_name + '/' + table_name + '/' + table_name + '.csv ' + \
		 						'-output /sql/temp/select_out'
		 			# print(cmd)
		 			os.system(cmd)
		 			cmd = 'hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-*streaming*.jar ' + \
		 						'-files "hdfs://localhost:9000/sql/' + db_name + '/' + table_name + '/schema.txt" ' + \
		 						'-file project_mapper.py ' + \
		 						'-mapper "python3 project_mapper.py ' + queries['project'] + '\" '+ \
		 						'-file remdup_reducer.py '+ \
		 						'-reducer "python3 remdup_reducer.py" ' + \
		 						'-input /sql/temp/select_out ' + \
		 						'-output /sql/final_out'
		 			os.system(cmd)
		 			exit()
		 		else:
		 			os.wait()
		elif(('project' in list_queries) and ('aggregate' in list_queries)):
				new_proc = os.fork()
				if(new_proc == 0):
		 			cmd = 'hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-*streaming*.jar ' + \
		 						'-files "hdfs://localhost:9000/sql/' + db_name + '/' + table_name + '/schema.txt" ' + \
		 						'-file identity_mapper.py ' + \
		 						'-mapper "python3 identity_mapper.py ' + queries['project'] + '\" ' + \
		 						'-file agg_reducer.py ' + \
		 						'-reducer "python3 agg_reducer.py'   + " " + queries['aggregate'][0] + "," +queries['aggregate'][1] + '\" ' + \
		 						'-input /sql/' + db_name + '/' + table_name + '/' + table_name + '.csv ' + \
		 						'-output /sql/final_out'
		 			print(cmd)
		 			os.system(cmd)
		 			exit()
				else:
		 			os.wait()
	read.read_folder("/sql/final_out/")
	delete.delete_folder("/sql/temp/select_out/ ")
	delete.delete_folder("/sql/final_out/ ")
		 		
