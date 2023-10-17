import os
import traceback
import pandas as pd
from tqdm import tqdm
from openpyxl import load_workbook
from utilities import get_path_to_df, get_vehicles_and_their_types
from Parser import Parser
from FuelTypesParser import FuelTypesParser
from SubFuelTypesParser import SubFuelTypesParser

"""
Preprocessing that needs to be done manually for this script to work:
    1. Check for merged cells in vehicle lines sheet and unmerge them.
    2. Add an empty row between model year tables.
    3. Remove repetitive model year from the model year's table (see Explorer)
    4. Add fuel types for every vehicle line in the config file.
    5. No empty line should be under vehicle line name in the sheet.
"""

vehicle_lines = ['Aviator', 'Bronco', 'Bronco Sport', 'Corsair',
                 'Econoline', 'Edge', 'Escape', 'Expedition',
                 'Explorer', 'Motorhome', 'F-150', 'Maverick', 'Medium Truck',
                 'Mustang', 'Mustang Mach E', 'Navigator', 'Ranger',
                 'Super Duty', 'Transit', 'Transit Connect']

# path to main excel file
path = get_path_to_df()
no_fuel_type_vehicles, fuel_type_vehicles, sub_fuel_type_vehicles = get_vehicles_and_their_types()
dir_to_save = 'csvs_generated/'  # to save new csvs
if not os.path.exists(dir_to_save):
    os.mkdir(dir_to_save)

with open('missing_csvs.txt', 'w') as f:
    for vehicle in tqdm(vehicle_lines):
        print(vehicle)

        wb = load_workbook(path)
        # read original sheet
       # df = pd.read_excel(path, vehicle, header=None)

        # initialize an object with correct corresponding class
        if vehicle in no_fuel_type_vehicles:
            parser_obj = Parser(wb, vehicle)
        if vehicle in fuel_type_vehicles:
            parser_obj = FuelTypesParser(wb, vehicle)
        if vehicle in sub_fuel_type_vehicles:
            parser_obj = SubFuelTypesParser(wb, vehicle)

        try:
            # parse the sheet and save a csv
            parser_obj = parser_obj.run(dir_to_save)
        except Exception as e:
            # write vehicle line names to txt files where the scrapper doesn't work
            f.write(vehicle + '\n')
            traceback.print_exc()
            print(f'error in {vehicle}, error: {e}')


