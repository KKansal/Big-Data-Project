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
fp = open("schema.txt","r")
schema = fp.read().strip("()")
# print(schema)
fp.close()

schema = schema.split(',')
schema = [x.split(':')[0] for x in schema]
schema_len = len(schema)


for i in range(0,query_len):
	for j in range(0,schema_len):
		if query[i] == schema[j]:
			column_order.append(j)

flag = True
for x in query:
	if x not in schema:
		print("Project: Given column doesnot exist")
		flag = False


for line in table:
	line = line.strip()
	if(line == "Select: Given column doesnot exist"):
		print(line)
		flag = False
	for i in range(len(column_order)):
		#print("col order: ",len(column_order))
		if i != len((column_order))-1:
			if flag:
				print(line.split(",")[column_order[i]],end=",")
		else:
			if flag:
				print(line.split(",")[column_order[i]])
