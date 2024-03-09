import pandas as pd
import numpy as np
temp = "Global_surface_temperature.csv"
ghg = "quarterly.csv"
sealevel = "sealevel.csv"
test_output = "test_output.sql"
cont_country = "country_continent_mapping.csv"



def create_data(filepath1, filepath2, filepath3, filepath4):

    # load data 
    temp_data = pd.read_csv(filepath1)
    ghg_data = pd.read_csv(filepath2)
    sealevel_data = pd.read_csv(filepath3)
    country_continent_map = pd.read_csv(filepath4)

    # data wrangling

    ## for greenhouse gas data
    ### note: the ghgdata is quarterly so we'll have to change the data type in sql
    ghg_data = ghg_data.drop(columns = ['ObjectId', 'ISO2', 'Indicator', 'Source', 
                                        'CTS Code', 'CTS Name', 'CTS Full Descriptor', 
                                        'Seasonal Adjustment', 'Scale', 'Gas Type'])
    ghg_data = pd.melt(ghg_data, id_vars=['Country', 'ISO3', 'Unit', 'Industry'],
                       var_name = "Year", value_name="Emissions")
    ghg_data['Emissions'] = round(ghg_data['Emissions'], 2)
    ghg_data['YearNoQ'] = ghg_data['Year'].apply(lambda x: x[0:4])

    ## for temperature data
    temp_data = temp_data.drop(columns=['ObjectId', 'ISO2', 'Indicator', 'Source', 'CTS Code', 
                                        'CTS Name', 'CTS Full Descriptor'])
    temp_data = pd.melt(temp_data, id_vars=['Country', 'ISO3', 'Unit'], var_name='Year', value_name='Temperature')
    temp_data = temp_data.dropna()
    temp_data['Temperature'] = round(temp_data['Temperature'], 2)
    temp_data = pd.merge(temp_data, country_continent_map, on = 'Country')
    temp_data['Country'] = temp_data['Country'].apply(lambda x: x.replace("'", ""))
    

    ## for sealevel data
    sealevel_data = pd.DataFrame(sealevel_data.groupby('Year', as_index=False)['GMSL_noGIA'].mean())
    sealevel_data['GMSL_noGIA'] = round(sealevel_data['GMSL_noGIA'], 2)

    return temp_data, ghg_data, sealevel_data


temp_data, ghg_data, sealevel_data = create_data(temp, ghg, sealevel, cont_country)

def create_sql(temp_data, ghg_data, sealevel_data, outputfilepath):
    # create input lists
    tables = ["Continent", "Country", "SeaLevel", "GHGEmission", "Temperature", 
              "Industry", "Emitted", "Produces", "TempChange"]
    
    produces_df = pd.DataFrame(ghg_data.groupby(['Year', 'Industry'], as_index= False)['Emissions'].sum())
    emmitted_df = pd.DataFrame(ghg_data.groupby(['Country', 'Year'], as_index = False)['Emissions'].sum())

    continent= [(ghg_data.at[i,'Country'], ghg_data.at[i, 'ISO3']) for i in range(len(ghg_data))]
    continent = list(set(continent))
    country = [(temp_data.at[i, 'Country'], temp_data.at[i, 'Continent'],
                temp_data.at[i, 'ISO3']) for i in range(len(temp_data))]
    country = list(set(country))
    sealevel = [(sealevel_data.at[i, 'Year'], "World", "mm", sealevel_data.at[i, 'GMSL_noGIA'])
                for i in range(len(sealevel_data))]
    ghg_emission = [(ghg_data.at[i, 'Year'], ghg_data.at[i, 'Unit'])
                    for i in range(len(ghg_data))]
    ghg_emission = list(set(ghg_emission))
    temperature = [(temp_data.at[i, 'Year'],(ghg_data.at[i, 'Year'] if temp_data.at[i, 'Year'] == 
                                             ghg_data.at[i, 'YearNoQ']
                                             else 0), temp_data.at[i, 'Unit'])
                                             for i in range(len(temp_data))]
    temperature = list(set(temperature))
    temperature = [i for i in temperature if i[1] != 0]

    industry = list(ghg_data['Industry'])
    industry = list(set(industry))
    
    emitted = [(emmitted_df.at[i, 'Country'], emmitted_df.at[i, 'Year'], emmitted_df.at[i, 'Emissions'])
               for i in range(len(emmitted_df))]
    produces = [(produces_df.at[i, 'Year'], produces_df.at[i, 'Industry'], produces_df.at[i, 'Emissions'])
                for i in range(len(produces_df))]
    tempchange = [(temp_data.at[i, 'Country'], temp_data.at[i, 'Year'],
                   (ghg_data.at[i, 'Year'] if temp_data.at[i, 'Year'] == 
                                             ghg_data.at[i, 'YearNoQ']
                                             else 0),  temp_data.at[i, 'Temperature'])
                                             for i in range(len(temp_data))]
    tempchange = [i for i in tempchange if i[2] != 0]

    # write to .sql file
    with open(outputfilepath, 'w') as outputfile:
        for table in tables:
            outputfile.write(f"drop table {table} cascade constraints;\n")
        outputfile.write(f"\ncreate table Continent (\n\tcontinentName varchar (50) primary key,\n\t"\
                         "code varchar2 (10)\n);\n")
        outputfile.write(f"\ncreate table Country (\n\tcountryName varchar (50) primary key,\n\t"\
                         "continentName varchar2 (50),\n\t"\
                         "iso3 varchar2(3),\n\t"\
                         "foreign key (continentName) references Continent\n);\n")
        outputfile.write(f"\ncreate table SeaLevel (\n\tSealevelYear integer primary key, \n\t"\
                         "continentName varchar2 (50) not null,\n\t"\
                         "unit varchar2 (10), \n\t"\
                         "GMSL decimal (10, 2),\n\t"\
                         "foreign key (continentName) references Continent\n);\n")
        outputfile.write(f"\ncreate table GHGEmission (\n\tghgYear varchar (6) primary key, \n\t"\
                         "unit varchar (50)\n);\n")
        outputfile.write(f"\ncreate table Temperature (\n\tyear integer, \n\t"\
                         "ghgYear varchar (6), \n\t"\
                         "unit varchar2 (50), \n\t" \
                         "primary key (year, ghgYear), \n\t"\
                         "foreign key (ghgYear) references GHGEmission \n);\n")       
        outputfile.write(f"\ncreate table Industry (\n\tindustryName varchar2 (70) primary key\n\t);\n")
        outputfile.write(f"\ncreate table Emitted (\n\tcontinentName varchar2 (50), \n\t" \
                         "ghgYear varchar (6), \n\t" \
                         "co2concentration decimal (10, 2), \n\t"\
                         "primary key (continentName, ghgYear), \n\t"\
                         "foreign key (continentName) references Continent, \n\t"\
                         "foreign key (ghgYear) references GHGEmission \n ); \n")
        outputfile.write(f"\ncreate table Produces (\n\tghgYear varchar (6), \n\t" \
                         "industryName varchar2 (70), \n\t"\
                         "totalco2concentration decimal (10, 2), \n\t"\
                         "primary key (ghgYear, industryName), \n\t" \
                         "foreign key (ghgYear) references GHGEmission, \n\t"\
                         "foreign key (industryName) references Industry\n);\n")
        outputfile.write(f"\ncreate table TempChange (\n\tcountryName varchar2 (50), \n\t"\
                         "year integer,\n\t"\
                         "ghgYear varchar (6), \n\t"\
                         "temperature decimal (10, 2), \n\t"\
                         "primary key (countryName, year, ghgYear),\n\t"\
                         "foreign key (countryName) references Country, \n\t" \
                         "foreign key (year, ghgYear) references Temperature\n);\n")
        
        for values in continent:
            outputfile.write(f"insert into Continent values ('{values[0]}', '{values[1]}');\n")
        for values in country:
            outputfile.write(f"insert into Country values ('{values[0]}', '{values[1]}', '{values[2]}');\n")
        for values in sealevel:
            outputfile.write(f"insert into SeaLevel values ({values[0]}, '{values[1]}', '{values[2]}',{values[3]});\n")
        for values in ghg_emission:
            outputfile.write(f"insert into GHGEmission values ('{values[0]}', '{values[1]}');\n")
        for values in temperature:
            outputfile.write(f"insert into Temperature values ({values[0]}, '{values[1]}', '{values[2]}');\n")
        for values in industry:
            outputfile.write(f"insert into Industry values ('{values}');\n")
        for values in emitted:
            outputfile.write(f"insert into Emitted values ('{values[0]}', '{values[1]}', {values[2]});\n")
        for values in produces:
            outputfile.write(f"insert into Produces values ('{values[0]}', '{values[1]}', {values[2]});\n")
        for values in tempchange:
            outputfile.write(f"insert into TempChange values ('{values[0]}', {values[1]}, '{values[2]}', {values[3]});\n")

create_sql(temp_data, ghg_data, sealevel_data, test_output)

