import yaml
import datetime
import gen_script


def read_RPT_DT():
    with open('./config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    if 'RPT_DT' in config:
        RPT_DT = config['RPT_DT']
    else:
        current_date = datetime.date.today()
        RPT_DT = current_date.strftime('%dd%mm%Y')
    return RPT_DT, config


if __name__ == '__main__':
    RPT_DT, config = read_RPT_DT()
    gen_script.gen_tmp_table_script(RPT_DT, config)