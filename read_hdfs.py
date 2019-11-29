from requests import get
from getpass import getuser
import json

uname = getuser()

file_path = input()
file_path=file_path.strip()

#input is a folder and reads all files except _SUCCESS 

#getting file names

a = get("http://localhost:9870/webhdfs/v1/"+file_path+"?user.name="+uname+"&op=LISTSTATUS")
l = a.text
d = json.loads(l)

'''

d['FileStatuses']['FileStatus'][0]['pathSuffix']
This is the code snippet to read just the file names in a given hadoop directory
This is run in a loop to read all

'''
num_files = len(d['FileStatuses']['FileStatus'])

for i in range(num_files):
	if d['FileStatuses']['FileStatus'][i]['pathSuffix'] != "_SUCCESS":
		file = file_path+d['FileStatuses']['FileStatus'][i]['pathSuffix'] 
		respons = get("http://localhost:9870/webhdfs/v1/"+file+"?user.name="+uname+"&op=OPEN")
		if 'RemoteException' in respons.text:
			print("Failed to read file. File not Found")
		else:
			print(respons.text,end="")
			# try:
			# 	for j in respons.text.split('\n'):
			# 		print(j)
			# except:
			# 	print("File Found but error in reading it")
