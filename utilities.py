import yaml


def get_path_to_df(config_path='./config.yaml'):
    with open(config_path, 'r') as yml_file:
        cfg = yaml.safe_load(yml_file)
    main_csv_path = cfg['paths']['path to main csv']
    return main_csv_path


def get_vehicles_and_their_types(config_path='./config.yaml'):
    with open(config_path, 'r') as yml_file:
        cfg = yaml.safe_load(yml_file)
    no_fuel_types = cfg['vehicle line types']['no fuel types']
    fuel_types = cfg['vehicle line types']['fuel types']
    sub_fuel_types = cfg['vehicle line types']['sub-fuel types']
    return no_fuel_types.strip(', '), fuel_types.strip(', '), sub_fuel_types.strip(', ')
