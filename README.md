# Utilities

## Functions and Its Uses:
### 1. read_file: <br>
__This Module enables user to Read a File or a Database into DataFrame.__<br>
The File can be a Flatfile or a Url of type csv, txt, xml or json.<br>
The Database can be of type MySQL, SQLite, PostgreSQL, Oracle, Teradata, Netezza or Hive.<br>
 
### 2. var_type:<br>
__This Function  checks whether the Database columns is:__
* Constant
* Boolen
* Numeric Continuous
* Numeric Categorical
* Date
* Unique
* Others
 
### 3. missing_value_treatment:
__This Function handle Missing Values in a Database.__<br>
__It can peform 2 types of Imputations:-__<br>
* __KNN (K-Nearest Neighbors) Based Treatment.__
* __Columnwise Treatment.__
 
### 4. check_inputdata_attributes_column_type:
__This Function checks the type of input dataframe columns.__<br>
__The types are:__
* Constant
* Boolean
* Numeric With Length=1
* Numeric Continuous
* Uniformly Spaced
* Non-Uniformly Spaced
* Uniformly Distributed
* Date
* Only Text
* Text with Number & Special Character
* Not Known<br>
 
### __Note:__
* If a Numeric column is Uniformaly Spaced and also Uniformly Distributed then the function will return "Uniformly Distributed".
