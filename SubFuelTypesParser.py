from FuelTypesParser import FuelTypesParser


class SubFuelTypesParser(FuelTypesParser):
    def __init__(self, wb, vehicle, start_row=2):
        super().__init__(wb, vehicle, start_row)
        self.list_of_sub_fuel_types = self.get_sub_fuel_tpyes(self.df.values[1])

    # get sub fuel types
    def get_sub_fuel_tpyes(self, second_row_in_df):
        list_of_sub_fuel_types = []
        for row_value in second_row_in_df[2:]:
            if self.ensure_there_is_no_none_type(row_value):
                list_of_sub_fuel_types.append(row_value)
        return list_of_sub_fuel_types

    # get list of sub fuel types column values to add in the df
    def get_sub_fuel_types_column_values(self, num_of_model_years_in_df):
        list_of_sub_fuel_types_column_values = []
        for model_year in range(num_of_model_years_in_df):
            for sub_fuel_type in self.list_of_sub_fuel_types:
                list_of_sub_fuel_types_column_values.append(sub_fuel_type)
        return list_of_sub_fuel_types_column_values

    # add the fuel types list to the concatenated df
    def add_sub_fuel_types_column_to_concatenated_df(self, concatenated_df, main_dict_keys):
        num_of_model_years_in_df = len(list(main_dict_keys))
        sub_fuel_type_column_values = self.get_sub_fuel_types_column_values(num_of_model_years_in_df)
        concatenated_df['sub_fuel_type'] = sub_fuel_type_column_values
        concatenated_df = self.rearrange_columns_in_concatenated_df(concatenated_df)
        return concatenated_df

    # get list of values for model year column
    def get_model_year_column_values_list(self, model_years_in_dict):
        list_of_model_year_column_values = []
        for model_year in model_years_in_dict:
            for fuel_types in range(len(self.list_of_sub_fuel_types)):
                list_of_model_year_column_values.append(model_year)
        return list_of_model_year_column_values

    # add serial key according to fuel types and sub fuel types which will be used as a primary key in DB
    def add_serial_key(self, concatenated_df):
        serial_keys = []
        for row in concatenated_df.values:
            serial_key = row[0].lower().replace(' ', '_') + '_' + row[1] + '_' + row[2] + '_' + row[3]  # at index 2, sub fuel types exists
            serial_keys.append(serial_key)
        concatenated_df['serial_key'] = serial_keys
        concatenated_df = self.rearrange_columns_in_concatenated_df(concatenated_df)
        return concatenated_df

    # overwrite the postprocessing function
    def postprocess_dictionary(self, main_dict):
        for model_year in main_dict.keys():
            subdict = main_dict[model_year]
            for column in subdict.keys():
                self.remove_extra_keywords_from_dict(subdict, column)
        list_of_dfs = self.append_list_of_dfs(main_dict)
        concatenated_df = self.concatenate_dfs(list_of_dfs)
        concatenated_df = self.add_model_year_to_concatenated_df(concatenated_df, main_dict.keys())
        concatenated_df = self.add_sub_fuel_types_column_to_concatenated_df(concatenated_df, main_dict.keys())
        concatenated_df = self.add_fuel_types_column_to_concatenated_df(concatenated_df)
        concatenated_df = self.add_vehicle_line_name_to_concatenated_df(concatenated_df, self.vehicle_line_name)
        concatenated_df = self.add_serial_key(concatenated_df)
        concatenated_df.fillna('', inplace=True)
        return concatenated_df
