# i tried to make what we had on google doc to code
# i think i did it correctly, we can use it for the sql stuff

    
# insert data from sealevel.csv
import pandas as pd


def fn(filepath1, filepath2, filepath3, outputfilepath):
    # open 3 files (Global_surface_temperature.csv, quarterly.csv, sealevel.csv)
    # with open(filepath1, 'r') as file1, open(filepath2, 'r') as file2, open(filepath3, 'r') as file3:
    #     # read data
    #     surface_temp_data = file1.read()
    #     greenhouse_gas_data = file2.read()
    #     global_sea_level_data = file3.read()

    # load data 
    temp_data = pd.read_csv(filepath1)
    ghg_data = pd.read_csv(filepath2)
    sealevel_data = pd.read_csv(filepath3)

    # data wrangling

    ## for greenhouse gas data
    ghg_data = ghg_data.drop(columns = ['ObjectId', 'ISO2', 'Indicator', 'Source', 
                                        'CTS Code', 'CTS Name', 'CTS Full Descriptor', 
                                        'Seasonal Adjustment', 'Scale'])
    ghg_data = pd.melt(ghg_data, id_vars=['Country', 'ISO3', 'Unit', 'Industry', 'Gas Type'],
                       var_name = "Year", value_name="Emissions")

    ## for temperature data
    temp_data = temp_data.drop(columns=['ISO2', 'Indicator', 'Source', 'CTS Code', 
                                        'CTS Name', 'CTS Full Descriptor'])
    temp_data = pd.melt(temp_data, id_vars=['Country', 'ISO3', 'Unit'], var_name='Year', value_name='Temperature')

    # create input lists
    tables = ["Continent", "Country", "SeaLevel", "GHGEmission", "Temperature", 
              "Industry", "Emitted", "Produces", "TempChange"]
    
    continent= [(ghg_data.at[i,'Country'], ghg_data.at[i, 'ISO3']) for i in range(len(ghg_data))]
    ghg_emission = [(ghg_data.at[i, 'Year'], ghg_data.at[i, 'Gas Type'], ghg_data.at[i, 'Unit'])
                    for i in range(len(ghg_data))]
    industry = list(ghg_data['Industry'])
    emitted = [(ghg_data.at[i, 'Country'], ghg_data.at[i, 'Year'], ghg_data.at[i, 'Emissions'])
               for i in range(len(ghg_data))]

    # write to .sql file
    with open(outputfilepath, 'w') as outputfile:
        for table in tables:
            outputfile.write(f"drop table {table} cascade constraints;\n")
        outputfile.write(f"\ncreate table Continent (\n\tcontinentName varchar (30) primary key,\n\t"\
                         "code varchar2 (3)\n);\n")
        outputfile.write(f"\ncreate table Country (\n\tcountryName varchar (30) primary key,\n\t"\
                         "continentName varchar2 (30),\n\t"\
                         "iso3 varchar2(3),\n\t"\
                         "foreign key (continentName) references Continent\n);\n")
        outputfile.write(f"\ncreate table SeaLevel (\n\tSealevelYear integer primary key, \n\t"\
                         "continentName varchar2 (30) not null,\n\t"\
                         "unit varchar2 (10),\n\t"\
                         "GMSL decimal (10, 2),\n\t"\
                         "foreign key (continentName) references Continent\n);\n")
        outputfile.write(f"\ncreate table GHGEmission (\n\tghgYear integer primary key, \n\t"\
                         "gasType varchar2 (50), \n\t"\
                         "unit varchar (10)\n);\n")
        outputfile.write(f"\ncreate table Temperature (\n\tyear integer, \n\t"\
                         "ghgYear integer, \n\t"\
                         "unit varchar2 (10), \n\t" \
                         "primary key (year, ghgYear), \n\t"\
                         "foreign key (ghgYear) references GHGEmission \n);\n")       
        outputfile.write(f"\ncreate table Industry (\n\tindustryName varchar2 (50) primary key\n\t);\n")
        outputfile.write(f"\ncreate table Emitted (\n\t continentName varchar2 (30), \n\t" \
                         "ghgYear integer, \n\t" \
                         "co2concentration decimal (10, 2), \n\t"\
                         "primary key (continentName, ghgYear) \n\t"\
                         "foreign key (continentName) references Continent, \n\t"\
                         "foreign key (ghgYear) references GHGEmission \n ); \n")
        outputfile.write(f"\ncreate table Produces (\n\t ghgYear integer, \n\t" \
                         "industryName varchar2 (50), \n\t"\
                         "totalco2concentration decimal (10, 2), \n\t"\
                         "primary key (ghgYear, industryName), \n\t" \
                         "foreign key (ghgYear) references GHGEmission, \n\t"\
                         "foreign key (industryName) references Industry\n);\n")
        outputfile.write(f"\ncreate table TempChange (\n\t countryName varchar2 (50), \n\t"\
                         "year integer,\n\t"\
                         "ghgYear integer, \n\t"\
                         "temperature decimal (10, 2), \n\t"\
                         "primary key (countryName, year, ghgYear),\n\t"\
                         "foreign key (countryName) references Country, \n\t" \
                         "foreign key (year, ghgYear) references Temperature\n);\n")
        
        for values in continent:
            outputfile.write(f"insert into Continent values ('{values[0]}', '{values[1]}');\n")

        for values in ghg_emission:
            outputfile.write(f"insert into GHGEmission values ({values[0]}, '{values[1]}', '{values[2]}');\n")
        for values in industry:
            outputfile.write(f"insert into Industry values ('{values}');\n")
        for values in emitted:
            outputfile.write(f"insert into Emitted values ('{values[0]}', {values[1]}, {values[2]});\n")


    
temp = "Global_surface_temperature.csv"
ghg = "quarterly.csv"
sealevel = "sealevel.csv"
test_output = "test_output.sql"



fn(temp, ghg, sealevel, test_output)

