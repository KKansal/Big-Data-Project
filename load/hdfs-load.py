#This python file takes database_name, schema, csv_file
#A database is created if not present already
#If database is already present, then throw error

# import the python subprocess module
"""
import subprocess
import time
from pydoop import hdfs
"""
from requests import get,put
from getpass import getuser
username = getuser()
#URL where the data of the SQL engine is stored

webhdfs = "http://localhost:9870/webhdfs/v1/sql/"

"""
def run_cmd(args_list):
	#Sample code to run this function
	#(ret, out, err)= run_cmd(['hdfs', 'dfs', '-ls', 'hdfs_file_path'])
	#ret : returns status of execution code 0 for successful
	#out : returns what output hadoop generated (format is bytes use decode to stringify it)
	#err : Any error message 
	print('Running system command: {0}'.format(' '.join(args_list)))
	proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	s_output, s_err = proc.communicate()
	s_return =  proc.returncode
	return s_return, s_output, s_err
"""
db_name,file_name,schema = input().split()

<<<<<<< HEAD
db = input()
data_file = input()
file_name = data_file.split("/")[-1]
schema = input()+"\n"
=======
check_db = get(webhdfs + db_name + "/?op=LISTSTATUS")
if('RemoteException' in check_db.json().keys()):
	#Database Does not Exists so create Database
	creat_db = put(webhdfs + db_name + "?user.name="+getuser()+"&op=MKDIRS")
	try:
		creat_db.json()['boolean'] # check if Folder was successfully created
	except Exception as e:
		print("Unable to Create DB")
		print(e)

check_file = get(webhdfs + db_name + '/' + file_name.split(".")[0] + "/?op=LISTSTATUS")
if(('RemoteException' in check_file.json().keys())==0):
	print(check_file.text)
	raise Exception("Table Schema Already exists")
else:
	creat_file = put(webhdfs + db_name + '/' + file_name.split(".")[0] + "?user.name="+getuser()+"&op=MKDIRS")
	fd = open(file_name,'r')
	put(webhdfs + db_name + '/' + file_name.split(".csv")[0] + '/'+ file_name + "?user.name="+getuser()+"&op=CREATE",fd.read().encode())
	put(webhdfs + db_name + '/' + file_name.split(".csv")[0] + '/'+ "schema.txt" + "?user.name="+getuser()+"&op=CREATE",schema.encode())




"""
>>>>>>> c793fc75167169b61897e018bef382c1fd9e0628
(ret, out, err) = run_cmd(['hdfs','dfs','-ls','/sql/'])
out = out.decode()
if not(ret) and out.find(db) != -1:
	print("database already exists")
elif not(ret) and out.find(db) == -1:
	run_cmd(['hdfs','dfs','-mkdir','/sql/'+db])
<<<<<<< HEAD
	run_cmd(['hdfs','dfs','-put',data_file,'/sql/'+db+'/'])
	#time.sleep(2)
	run_cmd(['hdfs','dfs','-mv',"/sql/"+db+"/"+file_name,'/sql/'+db+'/'+db+".csv"])
	run_cmd(['hdfs','dfs','-touch','/sql/'+db+'/'+db+"_scheme.csv"])
	try:
		with hdfs.open('/sql/'+db+'/'+db+"_scheme.csv", 'a') as f:
			data = schema
			f.write(data.encode())
	except:
		pass

=======
	run_cmd(['hdfs','dfs','-put','/sql/'+db+db])
	with hdfs.open('/sql/'+db+'/'+db, 'a') as f:
        f.write(data.encode())
"""
>>>>>>> c793fc75167169b61897e018bef382c1fd9e0628
