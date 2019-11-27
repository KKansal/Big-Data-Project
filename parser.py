# -*- coding: utf-8 -*-
"""
Created on Thu Nov 21 14:08:15 2019

@author: Hp
"""

import re
query=input("Enter your query:")
regex_load=r'(load|LOAD) [a-z A-Z 0-9]+/[a-z A-Z 0-9]+\.csv (AS|as)\s\(([a-z A-z]+:(string|int|integer|char|varchar|float|double|time|year)(,|))+\);'
regex_select=r'(select|SELECT)\s(\*|[a-z A-Z 0-9]+)\s(from|FROM)\s[a-z A-Z 0-9]+/[a-z A-Z 0-9]+\.csv(\s(where|WHERE)\s[a-z A-Z 0-9]+(\s|)=(\s|)([0-9]+|\'[a-z A-Z]+\')|);'
regex_delete=r'(delete|DELETE)\s(\*|[a-z A-Z 0-9]+)\s(from|FROM)\s[a-z A-Z 0-9]+/[a-z A-Z 0-9]+\.csv(\s(where|WHERE)\s[a-z A-Z 0-9]+(\s|)=(\s|)([0-9]+|\'[a-z A-Z]+\')|);'
if(re.search(regex_load,query)!=None):
    print(query)
elif(re.search(regex_select,query)!=None):
    print(query)
elif(re.search(regex_delete,query)!=None):
    print(query)
else:
    print("error: Please check u might have missed table name or semicolon or check syntax")