# i tried to make what we had on google doc to code
# i think i did it correctly, we can use it for the sql stuff
    
# insert data from sealevel.csv
def fn(filepath1, filepath2, filepath3, outputfilepath):
    # open 3 files (Global_surface_temperature.csv, quarterly.csv, sealevel.csv)
    with open(filepath1, 'r') as file1, open(filepath2, 'r') as file2, open(filepath3, 'r') as file3:
        # read data
        surface_temp_data = file1.read()
        greenhouse_gas_data = file2.read()
        global_sea_level_data = file3.read()

    # create statements (posted on google doc)
    insert_emitted = f"INSERT INTO Emitted (Country, CO2_concentration) VALUES {greenhouse_gas_data}"
    insert_produces = f"INSERT INTO Produces (Quarter, Industry) VALUES {greenhouse_gas_data}"
    insert_ghg_emission = f"INSERT INTO GHGEmission (Quarter, Gas_Type, Unit) VALUES {greenhouse_gas_data}"
    insert_temp_change = f"INSERT INTO TempChange (Country, Temperature) VALUES {surface_temp_data}"
    insert_temperature = f"INSERT INTO Temperature (Year, Quarter, Unit) VALUES {surface_temp_data}"
    insert_country = f"INSERT INTO Country (Country, ISO3) VALUES {surface_temp_data}"
    insert_industry = f"INSERT INTO Industry (Industry) VALUES {surface_temp_data}"
    insert_sea_level = f"INSERT INTO SeaLevel (Country, Year, GMSL_noGIA) VALUES {global_sea_level_data}"
    insert_continent = f"INSERT INTO Continent (Country, ISO3, Year) VALUES {global_sea_level_data}"

    # save to outputfilepath
    with open(outputfilepath, 'w') as output_file:
        output_file.write(f"{insert_emitted}\n")
        output_file.write(f"{insert_produces}\n")
        output_file.write(f"{insert_ghg_emission}\n")
        output_file.write(f"{insert_temp_change}\n")
        output_file.write(f"{insert_temperature}\n")
        output_file.write(f"{insert_country}\n")
        output_file.write(f"{insert_industry}\n")
        output_file.write(f"{insert_sea_level}\n")
        output_file.write(f"{insert_continent}\n")
