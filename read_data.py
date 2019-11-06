import pandas as pd
import numpy as np
from datetime import datetime
import mysql.connector
from mysql.connector import Error

#read the csv data in a pandas dataframe
data = pd.read_csv("data.csv", sep=";")

#Obtain the header of the table: timeStamp and each variable
header = [data.columns[0]]
header.extend(data.Variable.unique())
print(header)

#Obtain each single date in the data
TimeStampUnique = data.Timestamp.unique()

#Separate each column of the data in a differet variable
TimeStampCol = data.Timestamp
VariableCol = data.Variable
ValueCol = data.Value

dataModified=[] #Will have a list for each single date
for time in TimeStampUnique:
  currentRow= [time] #Create a list with the first element equal to the date
  currentRow.extend([ [] for i in range(len(header)-1) ])#Create a list for each variable available in the data
  for i in range(len(TimeStampCol)):#iterate over all the rows in the data
    if(TimeStampCol[i] == time):#If the date in the current iterated row of the data is equal to the current date
      #append the value of the variable of that row in the respective list of the varibale in the created row
      currentRow[header.index(VariableCol[i])].append(ValueCol[i])
  #add a new row to the response data
  dataModified.append(currentRow)

#In this loop, we look for the list of values of each variable on every date
for row in dataModified:
  for i in range(1,len(row)): #iterate over every list of variable values, not the date
    if(len(row[i])>0):
      #If the list have at least one value, the new value at the position is equal to the average of those values
      row[i] = int(sum(row[i]) / len(row[i]))
    else:
      #Else the list does not have any value, set to the string "null"
      row[i] = "null"
    
print(dataModified)
print()
print("------------------------------------------------------------------")
print()

#Create a list of queries to send to the database
queries=[]

#Create table query
create_table="""CREATE TABLE `uptimeanalytics`.`data` (
  `Fecha` DATETIME NOT NULL,"""
for i in range(1, len(header)):
  create_table+="\n  `" + header[i] + "`INT NULL,"
create_table += "\n  PRIMARY KEY (`Fecha`));"

queries.append(create_table)
print(create_table + "\n")

#Insert data query
for row in dataModified:
  mySql_insert_query = """INSERT INTO uptimeanalytics.data VALUES (\""""
  date = datetime.strptime(row[0], "%d-%m-%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
  mySql_insert_query += date + "\","
  for i in range(1,len(row)-1):
    mySql_insert_query += str(row[i]) +","
  mySql_insert_query += str(row[len(row)-1]) + ")"
  queries.append(mySql_insert_query)
  print(mySql_insert_query)
  
print("\n--------------------------------------------------------------------------------")

#connect to the database and send the queries
try:
  connection = mysql.connector.connect(host='localhost',database='uptimeanalytics',user='root',password='123456789')
  FileName = "Script.sql"
  file = open(FileName, "w")
  if connection.is_connected():
    cursor = connection.cursor()
      
    for query in queries:
      file.write(query+"\n")
      result = cursor.execute(query)
      connection.commit()
      print("Record inserted successfully into data table")
      
except Error as e:
  print("Error while connecting to MySQL", e)
finally:
  file.close()
  if (connection.is_connected()):
    cursor.close()
    connection.close()
    print("MySQL connection is closed")