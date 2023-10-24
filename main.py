import yaml
import datetime
import gen_script
import argparse


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
    parser = argparse.ArgumentParser(description='Description of your program')
    parser.add_argument('--arg1', type=int, help='Description of arg1')
    parser.add_argument('--arg2', type=str, help='Description of arg2')

    args = parser.parse_args()

    if args.arg1:
        print(f"Arg1 value: {args.arg1}")
    if args.arg2:
        print(f"Arg2 value: {args.arg2}")

