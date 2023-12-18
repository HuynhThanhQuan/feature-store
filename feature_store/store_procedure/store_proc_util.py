import sys
sys.path.append('..')

import os
import yaml
import util_func


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

    agg_content = []
    # Loop through feature file
    # Each feature replace:
    #   {TBL_NM} -> tbl_nm var
    #   CINS_TMP_CUSTOMER_{RPT_DT_TBL} -> src_tbl
    #   {RPT_DT} -> RPT_DT
    for feat in features:
        feat_fp = os.path.join(feat_template_fd, feat + '.sql')
        with open(feat_fp, 'r') as f:
            content = ''.join(f.readlines())
            _, _, sql_sec = util_func.extract_desc_yaml_section_from_string(content)
            sql_sec = sql_sec.replace('{TBL_NM}', tbl_nm)
            sql_sec = sql_sec.replace('CINS_TMP_CUSTOMER_{RPT_DT_TBL}',src_tbl)
            sql_sec = sql_sec.replace("'{RPT_DT}'", 'RPT_DT')
            agg_content.append(util_func.format_sql(sql_sec))
    agg_content = util_func.COMMIT_CHECKPOINT.join(agg_content).strip()
    agg_content = util_func.post_processing_sql(agg_content)

    # Read proc template
    # Replace ${FEATURE_SCRIPTS}$ -> agg_content
    proc_content = template_content.replace('${FEATURE_SCRIPTS}$', agg_content)

    # Write into file and store at script
    with open('script/proc.sql','w') as f:
        f.writelines(proc_content)
    print('Done - stored at script/proc.sql')

def run():
    config_data = read_yaml_config()
    template_content = read_template()
    fill_in_template(config_data, template_content)


if __name__ == '__main__':
    run()