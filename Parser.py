import pandas as pd
from datetime import datetime
import yaml


class Parser:
    def __init__(self, wb, vehicle_line_name, start_row=1):
        self.vehicle_line_name = vehicle_line_name
        self.df = self.get_unhidden_rows_from_df(wb, self.vehicle_line_name)
        self.start_row = start_row

    #   get unhidden rows from the sheet
    def get_unhidden_rows_from_df(self, wb, vehicle_line_name):
        ws = wb[vehicle_line_name]
        data = []
        for row in ws:
            if ws.row_dimensions[row[0].row].hidden == False:
                row_values = [cell.value for cell in row]
                data.append(row_values)
        df = pd.DataFrame(data)
        return df

    #   get vehicle name
    def get_vehicle_line_name(self, first_row_values):
        if isinstance(first_row_values[0], float):
            vehicle_line_name = first_row_values[1]  # for Explorer
            return vehicle_line_name
        vehicle_line_name = first_row_values[0]  # all the vehicle line names are at the first index of first row
        return vehicle_line_name

    # Creating main dict with model year(s) as keys and their
    # respective values would be a subdictionary of columns and
    # their values

    # initialize dictionary with model year
    def initialize_dict_with_model_year(self, first_value_of_row, total_dict):
        if self.sanitize_row_value(first_value_of_row) in ('22MY', '23MY', '24MY'):
            subdict = {}
            total_dict[first_value_of_row.strip()] = subdict

    # check for None type row value
    def ensure_there_is_no_none_type(self, row_value):
        if row_value is not None:
            return True
        return False

    # sanitize row to avoid repetition of row cleaning
    def sanitize_row_value(self, row_value):
        if self.ensure_there_is_no_none_type(row_value):
            return row_value.strip()

    # format column names
    def format_column_names(self, column_name):
        if column_name in 'LPO Paint':
            column_name = 'lpo_paint'
        return column_name.replace('S/L',
                                   'State & Local').replace('&', 'and').replace('Commerical',
                                                                                'Commercial').replace('- ', "").replace(" -",
                                                                                                      " ").replace("/",
                                                                                                " or ").strip().replace(" ", '_').lower()

    # check and convert datatime object
    def check_and_convert_datetime_object(self, row_value):
        if isinstance(row_value, datetime):
            row_value = row_value.strftime("%m/%d/%Y")
            return row_value
        return row_value

    # get subdict key
    def get_subdict_key(self, row_value, model_years_in_total_dict):
        if self.sanitize_row_value(row_value) not in model_years_in_total_dict:
            subdict_key = self.format_column_names(row_value)
            return subdict_key

    # initialize subdict with column names
    def initialize_subdict_with_column_name(self, row, total_dict, subdict):
        for row_value in row[1:]: # column names are always at 1st index
            row_value = self.check_and_convert_datetime_object(row_value)
            if self.ensure_there_is_no_none_type(row_value):
                subdict_key = self.format_column_names(row_value)
                subdict[subdict_key] = []
                break

    # get column values
    def append_list_of_column_values(self, row, total_dict, subdict):
        for row_value in row[2:]:  # column values begin from 2nd index
            row_value = self.check_and_convert_datetime_object(row_value)
            if self.ensure_there_is_no_none_type(row_value):
                subdict[list(subdict.keys())[-1]].append(row_value)

    # get keywords to compare
    def get_tuple_of_values_to_compare(self, main_dict_keys, config_path='./config.yaml'):
        with open(config_path, 'r') as yml_file:
            cfg = yaml.safe_load(yml_file)
        values_to_compare = tuple(cfg['values to compare'].split(', '))
        values_to_compare += tuple(main_dict_keys)
        values_to_compare += tuple(['', ' '])
        return values_to_compare

    def check_to_break_main_loop(self, row, main_dict_keys):
        values_to_compare = self.get_tuple_of_values_to_compare(main_dict_keys)
        #values_to_compare = ('Down Weeks', 'Allocation Quarter', 'Constrained Commodity Information',
                            # 'Constrained Commodity Information-Explorer', '', ' ') + tuple(main_dict_keys)
        for row_value in row:
            row_value = self.check_and_convert_datetime_object(row_value)
            if self.sanitize_row_value(row_value) in values_to_compare:
                return True
        return False

    # check for empty row
    def is_empty_row(self, row):
        empty_row = all(not row_value for row_value in row)
        return empty_row

    # function to get dictionary
    def parser(self):
        main_dict = {}
        subdict = {}
        for row in self.df.values[self.start_row:]:

            if self.check_to_break_main_loop(row, main_dict.keys()):
                if len(subdict.keys()) > 0:
                    main_dict[list(main_dict.keys())[-1]] = subdict
                break

            if not self.is_empty_row(row):
                self.initialize_dict_with_model_year(row[0], main_dict)
                self.initialize_subdict_with_column_name(row, main_dict, subdict)
                self.append_list_of_column_values(row, main_dict, subdict)
            else:
                if len(subdict.keys()) > 0:
                    main_dict[list(main_dict.keys())[-1]] = subdict
                    subdict = {}

        return main_dict

    # Clean the dictionary, convert it to df,
    # add model year and vehicle name columns

    # remove unwanted keywords from the dictionary
    def remove_extra_keywords_from_dict(self, subdict, subdict_key):
        for value in subdict[subdict_key]:
            if value.rstrip() in ('Updates highlighted in orange', 'Past dates highlighted in gray', 'State and Local') or value == ' ':
                subdict[subdict_key].remove(value)

    # convert the dict to df
    def append_list_of_dfs(self, total_dict):
        list_of_dfs = []
        for key in total_dict.keys():
            temp_df = pd.DataFrame.from_dict(total_dict[key], orient='index').transpose()
            list_of_dfs.append(temp_df)
        return list_of_dfs


    def concatenate_dfs(self, list_of_dfs):
        concatenated_df = pd.concat(list_of_dfs)
        concatenated_df.reset_index(drop=True, inplace=True)
        return concatenated_df

    # to rearrange newly added columns
    def rearrange_columns_in_concatenated_df(self, concatenated_df):
        columns = concatenated_df.columns.tolist()
        columns = columns[-1:] + columns[:-1]
        concatenated_df = concatenated_df[columns]
        return concatenated_df

    # add model year to the concatenated df
    def add_model_year_to_concatenated_df(self, concatenated_df, model_years_in_total_dict):
        list_of_model_years = list(model_years_in_total_dict)
        concatenated_df['model_year'] = list_of_model_years
        concatenated_df = self.rearrange_columns_in_concatenated_df(concatenated_df)
        return concatenated_df

    # add vehicle line name in the concatenated df
    def add_vehicle_line_name_to_concatenated_df(self, concatenated_df, vehicle_line_name):
        vehicle_line_name_list_to_add = []
        for i in range(len(concatenated_df)):
            vehicle_line_name_list_to_add.append(vehicle_line_name)
        concatenated_df['vehicle_line'] = vehicle_line_name_list_to_add
        concatenated_df = self.rearrange_columns_in_concatenated_df(concatenated_df)
        return concatenated_df

    # add sno, lastly
    def add_sno_in_concatenated_df(self, concatenated_df):
        sno_list = []
        for index in range(len(concatenated_df)):
            sno_list.append(index)
        concatenated_df['sno'] = sno_list
        concatenated_df = self.rearrange_columns_in_concatenated_df(concatenated_df)
        return concatenated_df

    # add serial key to be used as a primary key in DB
    def add_serial_key(self, concatenated_df):
        serial_keys = []
        for row in concatenated_df.values:
            serial_key = row[0].lower().replace(' ', '_') + '_' + row[1]  # at index 0, vehicle line name exists
                                                                        # at index 1, model year exists
            serial_keys.append(serial_key)
        concatenated_df['serial_key'] = serial_keys
        concatenated_df = self.rearrange_columns_in_concatenated_df(concatenated_df)
        return concatenated_df

    # function for postprocessing the dictionary
    def postprocess_dictionary(self, main_dict):
        for model_year in main_dict.keys():
            subdict = main_dict[model_year]
            for column in subdict.keys():
                self.remove_extra_keywords_from_dict(subdict, column)
        list_of_dfs = self.append_list_of_dfs(main_dict)
        concatenated_df = self.concatenate_dfs(list_of_dfs)
        concatenated_df = self.add_model_year_to_concatenated_df(concatenated_df, main_dict.keys())
        concatenated_df = self.add_vehicle_line_name_to_concatenated_df(concatenated_df, self.vehicle_line_name)
        concatenated_df = self.add_serial_key(concatenated_df)
        concatenated_df.fillna('', inplace=True)
        return concatenated_df

    def run(self, dir_to_save):
        name_to_save = self.vehicle_line_name.replace('-', '_').rstrip().replace(' ', '_').lower()
        dictionary_after_parsing = self.parser()
        final_df = self.postprocess_dictionary(dictionary_after_parsing)
        final_df.to_csv(dir_to_save + name_to_save + '.csv', index=False)


