original_query = input().strip().strip(";")

query_type = original_query.split()[0] #Valid Types - LOAD,DELETE,SELECT




if(query_type.upper()=="LOAD"):
#LOAD QUERY FORMAT - LOAD database_name/table_name.csv AS (column_name: datatype, column_name: datatype);
	try:
		reduced_query = original_query.split(query_type)[1]
		db_file_name,schema = reduced_query.split("AS")
		db_name,file_name = db_file_name.strip().split("/")
		schema = schema.strip("( )")
		# print(db_name)
		# print(file_name)
		# print(schema)


	except Exception as e:
		print(e)
		print("Invalid Query")


else:
	print("Query type Not Supported")

