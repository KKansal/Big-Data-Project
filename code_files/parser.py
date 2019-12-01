# -*- coding: utf-8 -*-
"""
Created on Thu Nov 21 14:08:15 2019

@author: Vivek
"""

import re
def parser(query):
   # query=input("Enter your query:")
    regex_load=r'(load|LOAD) \w+/\w+\.csv (AS|as)\s\(([a-z A-z]+:(string|int|integer|char|varchar|float|double|time|year)(,|))+\);'
    regex_select=r'(select|SELECT)\s(\w+|(\w+(,|))+)\s(from|FROM)\s\w+/\w+\.csv(\s(where|WHERE)\s(\w+(\s|)=(\s|)([0-9]+|\w+|)(,|))+|);'
    #regex_delete=r'(delete|DELETE)\s(\*|[a-z A-Z 0-9]+)\s(from|FROM)\s[a-z A-Z 0-9]+/[a-z A-Z 0-9]+\.csv(\s(where|WHERE)\s[a-z A-Z 0-9]+(\s|)=(\s|)([0-9]+|\'[a-z A-Z]+\')|);'
    regex_delete=r'(DELETE|delete)\s\w+\/\w+\.csv;'
    regex_aggr=r'(SELECT|select)\s(\w+(,|))+\s(from|FROM)\s\w+/\w+\.csv((\s(where|WHERE) (\w+=([0-9]+|\w+|)(,|))+)|) (AGGREGATE|aggregate) by (COUNT|SUM|MAX|MIN|count|sum|max|min);'
    if(re.search(regex_load,query)!=None):
        query=query.split()
        d={}
        database=query[1].split("/")[0]
        table=query[1].split("/")[1]
        if("AS" in query):
          index_as=query.index("AS")
        else:
             index_as=query.index("as")
        schema=query[index_as+1].split(";")[0]
        d["load"]=database+" "+table+" "+schema
        return(d)
    elif(re.search(regex_select,query)!=None):
        d={}
        query=query.split()
        
        if('where' in query):
            index=query.index('where')
            #l1.append(query[index+1].split(";")[0])
            d['select']=query[index+1].split(";")[0]
        elif('WHERE' in query):
            index=query.index('WHERE')
            d['select']=query[index+1].split(";")[0]
        d['project']=query[1]
        d['database']=query[3].split("/")[0]
        d['table']=query[3].split("/")[1].split(";")[0]
        return(d)
    elif(re.search(regex_delete,query)!=None):
        d={}
        query=query.split()
        d['delete']=query[1].split("/")[0]+" "+query[1].split("/")[1].split(";")[0]
        return(d)
        
    elif(re.search(regex_aggr,query)!=None):
        d={}
        query=query.split()
        if('where' in query):
            index=query.index('where')
            d[query[0]]=query[index+1].split(";")[0]
            
        elif('WHERE' in query):
            index=query.index('WHERE')
            d[query[0]]=query[index+1].split(";")[0]
        d['project']=query[1]
        if("AGGREGATE" in query):
            index_aggr=query.index("AGGREGATE")
            
        else:
            
            index_aggr=query.index("aggregate")
        #l3.extend([string,query[1],query[index_aggr+2].split(";")[0]])
        
        string="aggregate"
        d[string]=[query[1],query[index_aggr+2].split(";")[0]]
        d['database']=query[3].split("/")[0]
        d['table']=query[3].split("/")[1].split(";")[0]
        return(d)
    else:
        print("error: Please check u might have missed table name or semicolon or check syntax")