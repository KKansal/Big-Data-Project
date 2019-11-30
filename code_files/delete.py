from requests import delete
from getpass import getuser



def delete_folder(file_path):
	# file_path = input()
	# print(file_path)
	file_path=file_path.strip()
	respons = delete("http://localhost:9870/webhdfs/v1/sql/"+file_path+"?user.name="+getuser()+"&op=DELETE&recursive=true")
	if respons.json()['boolean'] == True:
		#print("File/folder deleted successfully")
		return True
	# print(respons.text)
	else:
		return False
# 	print("Failed to delete: File not present")