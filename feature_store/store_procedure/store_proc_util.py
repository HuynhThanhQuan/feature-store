import sys
sys.path.append('../sql/')

import os
import yaml


config_proc_fp = 'config/proc.yaml'
proc_template_fp = 'template/CASA_PROC.sql'
feat_template_fd = '../sql/template/feature'


def read_yaml_config():
    with open(config_proc_fp, 'r') as file:
        config = yaml.safe_load(file)
    return config


def read_template():
    with open(proc_template_fp, 'r') as f: 
        lines = f.readlines()
        content = ''.join(lines)
        return content
    

def fill_in_template(config_data, template_content):
    src_tbl = config_data['SRC_TBL']
    casa_section = config_data['CASA']
    tbl_nm = casa_section['TABLE_NAME']
    features = casa_section['FEATURE']

    content = []
    # Loop through feature file
    # Each feature replace:
    #   {TBL_NM} -> tbl_nm var
    #   CINS_TMP_CUSTOMER_{RPT_DT_TBL} -> src_tbl
    #   {RPT_DT} -> RPT_DT
    for feat in features:
        feat_fp = os.path.join(feat_template_fd, feat + '.sql')
        with open(feat_fp, 'r') as f:
            content = ''.join(f.readlines())


def run():
    config_data = read_yaml_config()
    template_content = read_template()
    fill_in_template(config_data, template_content)



if __name__ == '__main__':
    run()