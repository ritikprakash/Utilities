
# coding: utf-8

# <div style="display:block">
#     <div style="width: 20%; display: inline-block; text-align: left;">
#         <div class="crop">
#             <img src="https://media.licdn.com/dms/image/C510BAQHYx68wy1dIng/company-logo_400_400/0?e=1551916800&v=beta&t=ZPw68MeIuGLt9obXpIsiWiB1QcyaniHjGk9doyJulys" style="height:75px; margin-left:0px" />
#         </div>
#     </div>
#     <div style="width: 59%; display: inline-block">
#         <h1  style="text-align: center">Utilities</h1><br>
#         <div style="width: 90%; text-align: center; display: inline-block;"><i>Author:</i> <strong>NetworthCorp</strong> </div>
#     </div>
#     <div style="width: 20%; text-align: right; display: inline-block;">
#         <div style="width: 100%; text-align: left; display: inline-block;">
#             <i>Created: </i>
#             <time datetime="Enter Date" pubdate>December, 2018</time>
#         </div>
#     </div>
# </div>

# # Functions and Its Uses:
# ***
# ### 1. read_file: 
# __This Module enables user to Read a File or a Database into DataFrame.__<br>
# The File can be a Flatfile or a Url of type csv, txt, xml or json.<br>
# The Database can be of type MySQL, SQLite, PostgreSQL, Oracle, Teradata, Netezza or Hive.
# 
# ### 2. var_type:
# __This Function  checks whether the Database columns is:__
# * Constant
# * Boolen
# * Numeric Continuous
# * Numeric Categorical
# * Date
# * Unique
# * Others
# 
# ### 3. missing_value_treatment:
# __This Function handle Missing Values in a Database.__<br>
# __It can peform 2 types of Imputations:-__
# * __KNN (K-Nearest Neighbors) Based Treatment.__
# * __Columnwise Treatment.__
# 
# ### 4. check_inputdata_attributes_column_type:
# __This Function checks the type of input dataframe columns.__
# __The types are:__
# * Constant
# * Boolean
# * Numeric With Length=1
# * Numeric Continuous
# * Uniformly Spaced
# * Non-Uniformly Spaced
# * Uniformly Distributed
# * Date
# * Only Text
# * Text with Number & Special Character
# * Not Known<br>
# 
# __Note:__
# * If a Numeric column is Uniformaly Spaced and also Uniformly Distributed then the function will return "Uniformly Distributed".
# 

# In[8]:


import os.path
import urllib.parse
import pandas as pd
import json
from bs4 import BeautifulSoup
import requests
import sqlite3
import pymysql
from pyhive import hive
from thrift.transport import TSocket
import socket
from thrift.transport import TTransport
from fancyimpute import KNN
import numpy as np
import re
import math
from scipy import stats


# In[9]:


#Read_File

def check_flatfile(filename):
    extension = os.path.splitext(filename)[1][1:]
    return extension

def read_file(filename=None,source=None,dbtype=None,queryfile=None,hostip=None,portno=None,username1=None,passwrd=None,dbname=None):
    try:
        if source=='flatfile' or source=='url':
            y=check_flatfile(filename)
            if y=='json':
                with open(filename,'r') as json_file:
                    data=json.load(json_file)
                    return(pd.DataFrame(data))

            elif y=='csv':   
                data=pd.read_csv(filename,sep=';')
                return(data)

            elif y=='txt':
                data=pd.read_csv(filename,sep='\s+')
                return(data)

            elif y=='xml':
                r=requests.get(filename)
                xml_doc=r.text
                df=BeautifulSoup(xml_doc,"html5lib")
                data=df.prettify()
                print("This is Not a DataFrame!!")
                return(data)
                
            else:
                print("The Following File Format is Not Correct!!")
                
        elif source=='database':
            
            if dbtype=='sqlite':
                conn = sqlite3.connect(filename) #It connects to the database server.
                c = conn.cursor()
                with open(queryfile,'r') as inserts:
                    for statement in inserts:
                        c.execute(statement)
                data=c.fetchall()
                return(pd.DataFrame(data))
                conn.commit()
                conn.close()
                
            elif dbtype=='mysql'or dbtype=='postgresql' or dbtype=='oracle' or dbtype=='sqlserver' or dbtype=='teradata' or dbtype=='netezza':
                
                if(hostip!=None and portno!=None and username1!=None and passwrd!=None and dbname!=None):
                    try:
                        conn = pymysql.connect(host=hostip, port=portno, user=username1, password=passwrd, db=dbname)
                        c = conn.cursor()
                        with open(queryfile,'r') as inserts:
                            for statement in inserts:
                                c.execute(statement)
                        data=c.fetchall()
                        return(pd.DataFrame(list(data)))
                        conn.commit()
                        conn.close()
                        
                    except:
                        print("Entered Database Settings is Not Correct!!")
                        
            elif dbtype=='hive':
                
                if(hostip!=None and portno!=None and username1!=None and passwrd!=None and dbname!=None):
                    try:
                        conn = hive.connect(host=hostip, port=portno, username=username1, password=passwrd, database=dbname)
                        c = conn.cursor()
                        with open(queryfile,'r') as inserts:
                            for statement in inserts:
                                c.execute(statement)
                        data=c.fetchall()
                        return(pd.DataFrame(list(data)))
                        conn.commit()
                        conn.close()
                        
                    except:
                        print("Entered Database Settings is Not Correct!!")
                        
                else:
                    print("Please Enter all the Database Settings!!")
     
            else:
                print("Enter a Valid Database Type!!")   
        
        else:
            print("Please Enter a Valid File Type!!")
            
    except FileNotFoundError:
        print("File Not Found!!")

#Missing Value Treatment
        
def dist_count(dataset):
    value_counts_with_nan = dataset.value_counts(dropna=False)
    value_counts_without_nan = value_counts_with_nan.loc[value_counts_with_nan.index.dropna()]
    distinct_count_with_nan = value_counts_with_nan.count()
    
    if value_counts_without_nan.index.inferred_type == "mixed":
        raise TypeError('Not supported mixed type')
    
    result = distinct_count_with_nan
    return(result)

def var_type(filepath):
    
    data= pd.read_csv(filepath)
    vartype=pd.DataFrame(columns = ['name','type_col'])
    for column in data:
    
        distinct_count = dist_count(data[column])
        leng = len(data[column])

        if distinct_count <= 1:
            vartype=vartype.append({'name': column,'type_col':'Constant'},ignore_index=True)
        elif pd.api.types.is_bool_dtype(data[column]) or (distinct_count == 2 and pd.api.types.is_numeric_dtype(data[column])):
            vartype=vartype.append({'name': column,'type_col':'Bool'},ignore_index=True)
        elif pd.api.types.is_numeric_dtype(data[column]):           
            if (((distinct_count/leng)*100)>=20):
                vartype=vartype.append({'name': column,'type_col':'Numeric Continuous'},ignore_index=True)
            else:
                vartype=vartype.append({'name': column,'type_col':'Numeric Categorical'},ignore_index=True)
        elif pd.api.types.is_datetime64_dtype(data[column]):
            vartype=vartype.append({'name': column,'type_col':'Date'},ignore_index=True)
        elif distinct_count == leng:
            vartype=vartype.append({'name': column,'type_col':'Unique'},ignore_index=True)
        else:
            vartype=vartype.append({'name': column,'type_col':'Others'},ignore_index=True)

    return(vartype)

def vari_type(filepath):
    
    data= pd.read_csv(filepath)
    vartype=pd.DataFrame(columns = ['name','type_col'])
    for column in data:
    
        distinct_count = dist_count(data[column])
        leng = len(data[column])

        if distinct_count <= 1:
            vartype=vartype.append({'name': column,'type_col':'continuous'},ignore_index=True)
        elif pd.api.types.is_bool_dtype(data[column]) or (distinct_count == 2 and pd.api.types.is_numeric_dtype(data[column])):
            vartype=vartype.append({'name': column,'type_col':'continuous'},ignore_index=True)
        elif pd.api.types.is_numeric_dtype(data[column]):
            vartype=vartype.append({'name': column,'type_col':'continuous'},ignore_index=True)
        elif pd.api.types.is_datetime64_dtype(data[column]):
            vartype=vartype.append({'name': column,'type_col':'continuous'},ignore_index=True)
        elif distinct_count == leng:
            vartype=vartype.append({'name': column,'type_col':'continuous'},ignore_index=True)
        else:
            vartype=vartype.append({'name': column,'type_col':'categorical'},ignore_index=True)

    return(vartype)
        

def missing_value_treatment(source=None,imputation='knn',nearest_row=None,treatment=None,custom=None):
    
    dataset= pd.read_csv(source,header=0)
    NaN=dataset.isnull().values.any()

    if NaN==True:
        
        if imputation=='knn':
            treated_dataset= pd.DataFrame(KNN(k=nearest_row).complete(dataset))
            return(treated_dataset)
        
        elif imputation=='columnwise':
            dtype= vari_type(source)  #It is used to check if a column is categorical or continuous.
            
            for index, column in dtype.iterrows():
                
                if column['type_col']=='categorical':

                    if treatment==None or treatment=='mode': #Default treatment is 'Mode' for categorical columns.
                        dataset[column['name']]= dataset[column['name']].fillna(dataset[column['name']].mode())

                    elif treatment=='delete':
                        dataset= dataset.dropna(inplace=False)

                    elif treatment=='custom':
                        dataset[column['name']]= dataset[column['name']].fillna(custom)

                    elif treatment=='nothing':
                        dataset[column['name']]= dataset[column['name']]

                elif column['type_col']=='continuous':  

                    if treatment==None or treatment=='mean':  #Default treatment is 'Mean' for continuous columns.
                        dataset[column['name']]= dataset[column['name']].fillna(dataset[column['name']].mean())

                    elif treatment=='delete':
                        dataset= dataset.dropna(inplace=False)

                    elif treatment=='mode':
                        dataset[column['name']]= dataset[column['name']].fillna(dataset[column['name']].mode())

                    elif treatment=='median':
                        dataset[column['name']]= dataset[column['name']].fillna(dataset[column['name']].median())

                    elif treatment=='custom':
                        dataset[column['name']]= dataset[column['name']].fillna(custom)

                    elif treatment=='nothing':
                        dataset[column['name']]=dataset[column['name']]

                else:
                    print("Unsupported Database!!!")
                    
            return(dataset)
                    
        else:
            print("Please Enter a Correct Imputation!!")
                      
    else:
        print("The Dataset does not contain any Missing Values!!")
        
#Column Type

def uniform(source,column): #This function returns whether the numeric column is Uniformly Spaced/ Non-Uniformly Spaced /Uniformly Distributed.
    
    data=pd.read_csv(source,header=0)
    x=data[column].unique()
    x.sort()
    temp=pd.DataFrame(x)
    dif=temp.diff().dropna()
    mean=dif.mean()
    std=dif.std()
    uniform=0
    count=0
    uni=0
    nonuni=0
    for index, row in dif.iterrows():
        count=count+1
        if (row <= (mean+(1.5*std))).bool() and (row >= (mean-(1.5*std))).bool():
            uni=uni+1
        else:
            nonuni=nonuni+1

    floor=math.floor((count*85)/100)

    if uni<=floor:
        test1='Uniformly Spaced'
    else:
        test1='Non-Uniformly Spaced'
    
    df=data[column].dropna()
    total=0
    for i in range(10):
        dt=df.sample(30)
        x=stats.shapiro(dt)
        if x[1]>= 0.05:
            total=total+1
    
    if total>=6:
        test1='Uniformly Distributed'

    return(test1)
        

def check(test_str): #Check the string is only text

    pattern = r'[^\.a-z A-Z]'
    if re.search(pattern, test_str):
        #Character other then . a-z/A-Z was found
        return 1
    else:
        #No character other then . a-z/A-Z was found
        return 0
    
def check_text(source,column): #Returns the column is only text or not.
    df= pd.read_csv(source,header=0)
    count=df[column].apply(lambda x:check(x) if pd.notnull(x) else x).sum()
    if count==0:
        return('Only Text')
    else:
        return('Text with Number & Special Character')

def dist_count(dataset): #find total distinct value in a column
    value_counts_with_nan = dataset.value_counts(dropna=False)
    value_counts_without_nan = value_counts_with_nan.loc[value_counts_with_nan.index.dropna()]
    distinct_count_with_nan = value_counts_with_nan.count()
    
    if value_counts_without_nan.index.inferred_type == "mixed":
        raise TypeError('Not supported mixed type')
    
    result = distinct_count_with_nan
    return(result)

def check_inputdata_attributes_column_type(filepath):
    
    data= pd.read_csv(filepath)
    vartype=pd.DataFrame(columns = ['Name','Type_col'])
    for column in data:
    
        distinct_count = dist_count(data[column])
        leng = len(data[column]) 
        
        
        if distinct_count <= 1: #Check for constant column
            vartype=vartype.append({'Name': column,'Type_col':'Constant'},ignore_index=True)
            
        elif pd.api.types.is_bool_dtype(data[column]) or (distinct_count == 2): #Check boolean column
            vartype=vartype.append({'Name': column,'Type_col':'Boolean'},ignore_index=True)
        
        elif pd.api.types.is_numeric_dtype(data[column]): #check for numeric column
            if len(str(data[column].max()))==1:
                vartype=vartype.append({'Name': column,'Type_col':'Numeric With Length=1'},ignore_index=True)
                
            elif (((distinct_count/leng)*100)>=20):
                vartype=vartype.append({'Name': column,'Type_col':'Numeric Continuous'},ignore_index=True)
                
            elif uniform(filepath,column):
                vartype=vartype.append({'Name': column,'Type_col':uniform(filepath,column)},ignore_index=True)
                                                      
        elif pd.api.types.is_datetime64_dtype(data[column]): #check date type column
            vartype=vartype.append({'Name': column,'Type_col':'Date'},ignore_index=True)
                  
        elif check_text(filepath,column):
            vartype=vartype.append({'Name': column,'Type_col':check_text(filepath,column)},ignore_index=True)

        else:
            vartype=vartype.append({'Name': column,'Type_col':'Not Known'},ignore_index=True)
                
    return(vartype)

