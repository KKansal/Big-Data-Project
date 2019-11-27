import sys

table = sys.stdin


'''
query has comma seperated values like age,name

To run this code use:

python3 project_mapper.py < Employee_data.csv educ,gender

'''
#print(sys.argv)
query = sys.argv[1]
query = query.split(',')
query_len = len(query)
column_order = []
fp = open("employee_schema.txt","r")
schema = fp.read()
fp.close()
schema = schema.split(',')
schema = [x.split(':')[0] for x in schema]
schema_len = len(schema)

for i in range(0,query_len):
	for j in range(0,schema_len):
		if query[i] == schema[j]:
			column_order.append(j)

for line in table:
	line = line.strip()
	for i in range(len(column_order)):
		#print("col order: ",len(column_order))
		if i != len((column_order))-1:
			print(line.split(",")[column_order[i]],end=",")
		else:
			print(line.split(",")[column_order[i]])
