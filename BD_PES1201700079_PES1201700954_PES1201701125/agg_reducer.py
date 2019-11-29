#the program should be executed as python agg_reducer.py < data.csv col_name,agg 

import sys

class agg_func():

    func_list = ["sum","count","min","max"]

    def __init__(self,func_name):
        self.agg_value = float('-inf')

        if(func_name not in agg_func.func_list):
          raise Exception("No function named " + func_name)
        self.func_name = func_name

    def sum(self,x):
        if(self.agg_value==float("-inf")):
            self.agg_value = int(x)
        else:
            self.agg_value +=int(x)

    def count(self,x):
        if(self.agg_value==float("-inf")):
            self.agg_value = 1
        else:
            self.agg_value +=1 

    
    def min(self,x):
        if(self.agg_value==float("-inf")):
            self.agg_value = int(x)
        else:
            self.agg_value = min(self.agg_value,int(x))
    
    def max(self,x):
        if(self.agg_value==float("-inf")):
            self.agg_value = int(x)
        else:
            self.agg_value = max(self.agg_value,int(x))


    def call_func(self,x):
        if(self.func_name == "sum"):
            self.sum(x)
        elif(self.func_name == "count"):
            self.count(x)
        elif(self.func_name == "min"):
            self.min(x)
        elif(self.func_name == "max"):
            self.max(x)


inline = sys.stdin


column,agg = sys.argv[1].split(",")


fd_schema = open('schema.txt','r')
schema = fd_schema.read().strip("()")
fd_schema.close()

schema = schema.split(',')
schema = list(map(lambda x: x.split(":")[0],schema))
# print(schema)

try:
    column_index = schema.index(column)
except:
    print("Column Name does not exist in the Schema")
    exit()


aggobj = agg_func(agg)

# prev_value = ''

for line in inline:
    line = [i.strip() for i in line.split(",")]
    new_value = line[column_index]
    try:
        aggobj.call_func(new_value)
        # if(prev_value == ''):                    #only for the first line
        #     prev_value = new_value
        #     aggobj.call_func(new_value)   

        # elif(prev_value==new_value):
        #     aggobj.call_func(new_value)
        # else:
        #     print(prev_value,aggobj.agg_value)
        #     prev_value = new_value
        #     aggobj.agg_value = float('-inf')
        #     aggobj.call_func(new_value)

    except ValueError:
        print("Cannot Perform operation on String Type")
        exit()
print(aggobj.agg_value)