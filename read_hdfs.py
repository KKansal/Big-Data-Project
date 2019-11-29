from requests import get
from getpass import getuser

uname = getuser()

file_path = input()
file_path=file_path.strip()

respons = get("http://localhost:9870/webhdfs/v1/"+file_path+"?user.name="+uname+"&op=OPEN")

if 'RemoteException' in respons.text:
	print("Failed to read file. File not Found")
else:
	try:
		for i in respons.text.split('\n'):
			print(i)
	except:
		print("File Found but error in reading it")
