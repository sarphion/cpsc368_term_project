# i tried to make what we had on google doc to code
# i think i did it correctly, we can use it for the sql stuff

    
# insert data from sealevel.csv
import pandas as pd


def fn(filepath1, filepath2, filepath3, outputfilepath):

    # load data 
    temp_data = pd.read_csv(filepath1)
    ghg_data = pd.read_csv(filepath2)
    sealevel_data = pd.read_csv(filepath3)

    # data wrangling

    ## for greenhouse gas data
    ### note: the ghgdata is quarterly so we'll have to change the data type in sql
    ghg_data = ghg_data.drop(columns = ['ObjectId', 'ISO2', 'Indicator', 'Source', 
                                        'CTS Code', 'CTS Name', 'CTS Full Descriptor', 
                                        'Seasonal Adjustment', 'Scale'])
    ghg_data = pd.melt(ghg_data, id_vars=['Country', 'ISO3', 'Unit', 'Industry', 'Gas Type'],
                       var_name = "Year", value_name="Emissions")
    ghg_data['Emissions'] = round(ghg_data['Emissions'], 2)
    produces_df = pd.DataFrame(ghg_data.groupby(['Year', 'Industry'], as_index= False)['Emissions'].sum())
    ghg_data['YearNoQ'] = ghg_data['Year'].apply(lambda x: x[0:4])

    ## for temperature data
    temp_data = temp_data.drop(columns=['ObjectId', 'ISO2', 'Indicator', 'Source', 'CTS Code', 
                                        'CTS Name', 'CTS Full Descriptor'])
    temp_data = pd.melt(temp_data, id_vars=['Country', 'ISO3', 'Unit'], var_name='Year', value_name='Temperature')
    temp_data['Temperature'] = round(temp_data['Temperature'], 2)


    ## for sealevel data
    sealevel_data['GMSL_noGIA'] = round(sealevel_data['GMSL_noGIA'], 2)

    # create input lists
    tables = ["Continent", "Country", "SeaLevel", "GHGEmission", "Temperature", 
              "Industry", "Emitted", "Produces", "TempChange"]
    
    continent= [(ghg_data.at[i,'Country'], ghg_data.at[i, 'ISO3']) for i in range(len(ghg_data))]
    sealevel = [(sealevel_data.at[i, 'Year'], "World", "mm", sealevel_data.at[i, 'GMSL_noGIA'])
                for i in range(len(sealevel_data))]
    ghg_emission = [(ghg_data.at[i, 'Year'], ghg_data.at[i, 'Gas Type'], ghg_data.at[i, 'Unit'])
                    for i in range(len(ghg_data))]
    temperature = [(temp_data.at[i, 'Year'],(ghg_data.at[i, 'Year'] if temp_data.at[i, 'Year'] == 
                                             ghg_data.at[i, 'YearNoQ']
                                             else "" ), temp_data.at[i, 'Unit'])
                                             for i in range(len(temp_data))]
    industry = list(ghg_data['Industry'])
    emitted = [(ghg_data.at[i, 'Country'], ghg_data.at[i, 'Year'], ghg_data.at[i, 'Emissions'])
               for i in range(len(ghg_data))]
    produces = [(produces_df.at[i, 'Year'], produces_df.at[i, 'Industry'], produces_df.at[i, 'Emissions'])
                for i in range(len(produces_df))]
    tempchange = [(temp_data.at[i, 'Year'],(ghg_data.at[i, 'Year'] if temp_data.at[i, 'Year'] == 
                                             ghg_data.at[i, 'YearNoQ']
                                             else "" ), temp_data.at[i, 'Unit'], temp_data.at[i, 'Temperature'])
                                             for i in range(len(temp_data))]


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
                         "unit varchar2 (10), \n\t"\
                         "GMSL decimal (10, 2),\n\t"\
                         "foreign key (continentName) references Continent\n);\n")
        outputfile.write(f"\ncreate table GHGEmission (\n\tghgYear integer primary key, \n\t"\
                         "gasType varchar2 (50), \n\t"\
                         "unit varchar (10)\n);\n")
        outputfile.write(f"\ncreate table Temperature (\n\tyear integer, \n\t"\
                         "ghgYear varchar (4), \n\t"\
                         "unit varchar2 (10), \n\t" \
                         "primary key (year, ghgYear), \n\t"\
                         "foreign key (ghgYear) references GHGEmission \n);\n")       
        outputfile.write(f"\ncreate table Industry (\n\tindustryName varchar2 (50) primary key\n\t);\n")
        outputfile.write(f"\ncreate table Emitted (\n\t continentName varchar2 (30), \n\t" \
                         "ghgYear varchar (4), \n\t" \
                         "co2concentration decimal (10, 2), \n\t"\
                         "primary key (continentName, ghgYear) \n\t"\
                         "foreign key (continentName) references Continent, \n\t"\
                         "foreign key (ghgYear) references GHGEmission \n ); \n")
        outputfile.write(f"\ncreate table Produces (\n\t ghgYear varchar (4), \n\t" \
                         "industryName varchar2 (50), \n\t"\
                         "totalco2concentration decimal (10, 2), \n\t"\
                         "primary key (ghgYear, industryName), \n\t" \
                         "foreign key (ghgYear) references GHGEmission, \n\t"\
                         "foreign key (industryName) references Industry\n);\n")
        outputfile.write(f"\ncreate table TempChange (\n\t countryName varchar2 (50), \n\t"\
                         "year integer,\n\t"\
                         "ghgYear varchar (4), \n\t"\
                         "temperature decimal (10, 2), \n\t"\
                         "primary key (countryName, year, ghgYear),\n\t"\
                         "foreign key (countryName) references Country, \n\t" \
                         "foreign key (year, ghgYear) references Temperature\n);\n")
        
        for values in continent:
            outputfile.write(f"insert into Continent values ('{values[0]}', '{values[1]}');\n")
        for values in sealevel:
            outputfile.write(f"insert into SeaLevel values ({values[0]}, '{values[1]}', '{values[2]}',{values[3]});\n")
        for values in ghg_emission:
            outputfile.write(f"insert into GHGEmission values ('{values[0]}', '{values[1]}', '{values[2]}');\n")
        for values in temperature:
            outputfile.write(f"insert into Temperature values ({values[0]}, '{values[1]}', '{values[2]}');\n")
        for values in industry:
            outputfile.write(f"insert into Industry values ('{values}');\n")
        for values in emitted:
            outputfile.write(f"insert into Emitted values ('{values[0]}', '{values[1]}', {values[2]});\n")
        for values in produces:
            outputfile.write(f"insert into Produces values ('{values[0]}', '{values[1]}', {values[2]});\n")
        for values in tempchange:
            outputfile.write(f"insert into TempChange values ({values[0]}, '{values[1]}', '{values[2]}', {values[3]});\n")

temp = "Global_surface_temperature.csv"
ghg = "quarterly.csv"
sealevel = "sealevel.csv"
test_output = "test_output.sql"



fn(temp, ghg, sealevel, test_output)

