#This python file takes database_name, schema, csv_file
#A database is created if not present already
#If database is already present, then throw error

# import the python subprocess module
import subprocess
import time
from pydoop import hdfs


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

db = input()
data_file = input()
file_name = data_file.split("/")[-1]
schema = input()+"\n"
(ret, out, err) = run_cmd(['hdfs','dfs','-ls','/sql/'])
out = out.decode()
if not(ret) and out.find(db) != -1:
	print("database already exists")
elif not(ret) and out.find(db) == -1:
	run_cmd(['hdfs','dfs','-mkdir','/sql/'+db])
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

