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

    temp_data = pd.read_csv(filepath1)
    ghg_data = pd.read_csv(filepath2)
    sealevel_data = pd.read_csv(filepath3)

    tables = ["Continent", "Country", "SeaLevel", "GHGEmission", "Temperature", 
              "Industry", "Emitted", "Produces", "TempChange"]
    
    industry = list(ghg_data['Industry'])


    with open(outputfilepath, 'w') as outputfile:
        for table in tables:
            outputfile.write(f"drop table {table} cascade constraints;\n")
        outputfile.write(f"\ncreate table Continent (\n\tcontinentName varchar (30) primary key,\n\t"\
                         "code varchar2 (3)\n);\n")
        outputfile.write(f"\ncreate table Country (\n\tcountryName varchar (30) primary key,\n\t"\
                         "continentName varchar2 (30),\n\t"\
                         "iso3 varchar2(3),\n\t"\
                         "foreign key (continentName) references Continent\n);\n")
        outputfile.write(f"\ncreate table Industry (\n\tindustryName varchar2 (50) primary key\n\t);\n")
        for values in industry:
            outputfile.write(f"insert into Industry values ('{values}');\n")

    # create statements (posted on google doc)
    # insert_emitted = f"INSERT INTO Emitted (Country, CO2_concentration) VALUES {greenhouse_gas_data}"
    # insert_produces = f"INSERT INTO Produces (Quarter, Industry) VALUES {greenhouse_gas_data}"
    # insert_ghg_emission = f"INSERT INTO GHGEmission (Quarter, Gas_Type, Unit) VALUES {greenhouse_gas_data}"
    # insert_temp_change = f"INSERT INTO TempChange (Country, Temperature) VALUES {surface_temp_data}"
    # insert_temperature = f"INSERT INTO Temperature (Year, Quarter, Unit) VALUES {surface_temp_data}"
    # insert_country = f"INSERT INTO Country (Country, ISO3) VALUES {surface_temp_data}"
    # insert_industry = f"INSERT INTO Industry (Industry) VALUES {surface_temp_data}"
    # insert_sea_level = f"INSERT INTO SeaLevel (Country, Year, GMSL_noGIA) VALUES {global_sea_level_data}"
    # insert_continent = f"INSERT INTO Continent (Country, ISO3, Year) VALUES {global_sea_level_data}"

    # # save to outputfilepath
    # with open(outputfilepath, 'w') as output_file:
    #     output_file.write(f"{insert_emitted}\n")
    #     output_file.write(f"{insert_produces}\n")
    #     output_file.write(f"{insert_ghg_emission}\n")
    #     output_file.write(f"{insert_temp_change}\n")
    #     output_file.write(f"{insert_temperature}\n")
    #     output_file.write(f"{insert_country}\n")
    #     output_file.write(f"{insert_industry}\n")
    #     output_file.write(f"{insert_sea_level}\n")
    #     output_file.write(f"{insert_continent}\n")


    
temp = "Global_surface_temperature.csv"
ghg = "quarterly.csv"
sealevel = "sealevel.csv"
test_output = "test_output.sql"



fn(temp, ghg, sealevel, test_output)