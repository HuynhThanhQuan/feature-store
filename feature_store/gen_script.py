import os
import logging
import util, util_func
import yaml

logger = logging.getLogger(__name__)


SQL_TEMPLATE = 'sql/template'
SQL_SCRIPT = 'sql/script'


def read_sql_gen_script_config():
    internal_config = None
    with open('sql/config/script.yaml', 'r') as file:
        internal_config = yaml.safe_load(file)
    return internal_config


def mkdir_report_date_folder(RPT_DT):
    fp = os.path.join(SQL_SCRIPT, RPT_DT)
    if not os.path.exists(fp):
        os.makedirs(fp)
    return fp


def get_sql_file(fp):
    if fp.endswith('.sql'):
        return fp
    return fp + '.sql'


def get_rpt_dt_table_name(tbl_nm, RPT_DT_TBL):
    if RPT_DT_TBL in tbl_nm:
        return tbl_nm
    return tbl_nm + '_' + RPT_DT_TBL


@util.timeit
def gen_table_script(response):
    logger.info('Generating TMP table scripts - Preparing')
    logger.info('Reading internal config')
    internal_config = read_sql_gen_script_config()
    table_names = internal_config['TABLE']

    logger.info('Reading request config')
    RPT_DT, config = response['RPT_DT'], response['config']
    RPT_DT_TBL = response['RPT_DT_TBL']

    # Setup
    RPT_DT_FP = mkdir_report_date_folder(RPT_DT)
    logger.info('REPORT_DATE_FOLDER: ' + RPT_DT_FP)
  
    rpt_tables = []
    # Loop through all files in the table template folder
    for tbl_nm in table_names:
        # Read the template file, respecting to Table Name
        file_content = None
        with open(os.path.join(SQL_TEMPLATE, 'table', get_sql_file(tbl_nm)), "r") as file:
            file_content = file.read()
            # Replace the patterns with the values above
            file_content = file_content.replace("{RPT_DT}", RPT_DT)
            file_content = file_content.replace("{RPT_DT_TBL}", RPT_DT_TBL)
        # Write the updated content back to the file placed at script folder
        tbl_dt = get_sql_file(get_rpt_dt_table_name(tbl_nm, RPT_DT_TBL))
        with open(os.path.join(RPT_DT_FP, tbl_dt), "w") as file:
            file.write(file_content)
            logger.debug('Created file: ' + tbl_dt)
                
    logger.info('Generated INSERT TMP table scripts.')
    logger.info('Generate TMP Table Script completed.')
    return {'RPT_TABLE': rpt_tables}


@util.timeit
def gen_feature_script(response):
    logger.info('Generating Feature scripts - Preparing')
    logger.info('Reading internal config')
    internal_config = read_sql_gen_script_config()
    feature_names = internal_config['FEATURE']

    logger.info('Reading request config')
    RPT_DT, config = response['RPT_DT'], response['config']
    RPT_DT_TBL = response['RPT_DT_TBL']
    FS_TBL_NM = config['FEATURE_STORE_TBL'] if 'FEATURE_STORE_TBL' in config else None

    # Setup
    RPT_DT_FP = mkdir_report_date_folder(RPT_DT)
    logger.info('REPORT_DATE_FOLDER: ' + RPT_DT_FP)

    # Loop through all files in the UNSTRUCTURED placeholder
    for ft_nm in feature_names:
        # Read the file
        with open(os.path.join(SQL_TEMPLATE, 'feature', get_sql_file(ft_nm)), "r") as file:
            file_content = file.read()
        # Replace the placeholders with the values above
        file_content = file_content.replace("{RPT_DT}", RPT_DT)
        file_content = file_content.replace("{RPT_DT_TBL}", RPT_DT_TBL)
        if FS_TBL_NM is not None:
            file_content = file_content.replace("{TBL_NM}", FS_TBL_NM)
        # Write the updated content back to the file
        ft_dt = get_sql_file(ft_nm)
        with open(os.path.join(RPT_DT_FP, ft_dt), "w") as file:
            file.write(file_content)
            logger.debug('Created file: ' + ft_dt)
    logger.info('Generate Feature Script completed.')
    return {}


def aggregate_sql_scripts(response):
    RPT_DT, _ = response['RPT_DT'], response['config']
    RPT_DT_TBL = response['RPT_DT_TBL']

    internal_config = read_sql_gen_script_config()
    table_names = internal_config['TABLE']
    table_names = [get_rpt_dt_table_name(tbl_nm, RPT_DT_TBL) for tbl_nm in table_names]
    feature_names = internal_config['FEATURE']
    sql_files =  table_names + feature_names

    # Setup
    RPT_DT_FP = mkdir_report_date_folder(RPT_DT)
    logger.info('REPORT_DATE_FOLDER: ' + RPT_DT_FP)

    script_seq = []
    for sql_file in sql_files:
        with open(os.path.join(RPT_DT_FP, get_sql_file(sql_file)), "r") as file:
            content = file.read()
            script_seq.append(content)

    agg_scripts = util_func.COMMIT_CHECKPOINT.join(script_seq)
    agg_scripts = agg_scripts + util_func.COMMIT_CHECKPOINT
    agg_scripts = agg_scripts.strip()

    logger.debug(f'Final aggregated scripts {agg_scripts}')
    agg_fp = f"./output/dev_fs_{RPT_DT_TBL}.sql"
    with open(agg_fp, 'w') as f:
        f.writelines(agg_scripts)
    logger.info(f'Final [DEV] aggregated scripts saved at {agg_fp}')
    
    # Convert DW_ANALYTICS to DWPROD for production
    prod_agg_scripts = agg_scripts.replace('DW_ANALYTICS', 'DWPROD')
    prod_agg_fp = f"./output/prod_fs_{RPT_DT_TBL}.sql"
    with open(prod_agg_fp, 'w') as f:
        f.writelines(prod_agg_scripts)
    logger.info(f'Final [PROD] aggregated scripts saved at {prod_agg_fp}')
    
    return {
        'DEV_FS': {
            'script': agg_scripts,
            'filepath': agg_fp
        },
        'PROD_FS': {
            'script': prod_agg_scripts,
            'filepath': prod_agg_fp
        }
    }