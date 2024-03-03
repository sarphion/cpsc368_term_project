import pandas as pd
import cx_oracle
import csv

# reference i used:
# https://stackoverflow.com/questions/21140590/dynamic-insert-statement-in-python

# define the table names
sealevel_table_name = 'sealevel'
surf_temp_table_name = 'surfacetemp'
quarterly_table_name = 'quarterly'

# define CSV file paths
sealevel_csv_path = 'sealevel.csv'
surf_temp_csv_path = 'Global_surface_temperature.csv'
quarterly_csv_path = 'quarterly.csv'

# establish db connection (change this depending on who's running sql part)
connection = cx_oracle.connect('username/password@hostname:port/service_name')

# create cursor
cursor = connection.cursor()

# function to insert data from CSV file into database table
def insert_data(table_name, csv_path):
    # open CSV file
    with open(csv_path, 'r') as file:
        # create reader object
        reader = csv.reader(file)
        
        # skip header row
        next(reader)
        
        # iterate over each row in the file
        for row in reader:
            # extract values from row
            values = tuple(row)
            
            # construct INSERT statement
            sql = f"INSERT INTO {table_name} VALUES {values}"
            
            # execute SQL statement
            cursor.execute(sql)
        
        # commit changes to db
        connection.commit()

# use function for each dataset separately 
        
# insert data from sealevel.csv
insert_data(sealevel_table_name, sealevel_csv_path)

# insert data from Global_surface_temperature.csv
insert_data(surf_temp_table_name, surf_temp_csv_path)

# insert data from quarterly.csv
insert_data(quarterly_table_name, quarterly_csv_path)

# close cursor and db connection
cursor.close()
connection.close()