import os
import logging
import util
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
def gen_tmp_table_script(response):
    logger.info('Generating TMP table scripts - Preparing')
    internal_config = read_sql_gen_script_config()
    table_names = internal_config['TABLE']
    RPT_DT, config = response['RPT_DT'], response['config']
    RPT_DT_TBL = response['RPT_DT_TBL']
    
    RPT_DT_FP = mkdir_report_date_folder(RPT_DT)

    logger.info('REPORT_DATE_FOLDER: ' + RPT_DT_FP)
  
    rpt_tables = []
    # Loop through all files in the table template folder
    for tbl_nm in os.listdir(table_names):
        if tbl_nm.endswith(".sql"):
            # Read the template file, respecting to Table Name
            with open(os.path.join(SQL_TEMPLATE, get_sql_file(tbl_nm)), "r") as file:
                file_content = file.read()
            # Replace the patterns with the values above
            file_content = file_content.replace("{RPT_DT}", RPT_DT)
            file_content = file_content.replace("{RPT_DT_TBL}", RPT_DT_TBL)
            # Write the updated content back to the file placed at script folder
            tbl_dt = get_sql_file(get_rpt_dt_table_name(tbl_nm, RPT_DT_TBL))
            with open(os.path.join(RPT_DT_FP, tbl_dt), "w") as file:
                file.write(file_content)
                logger.debug('Created file: ' + os.path.join(RPT_DT, 'insert', tbl_dt))
                
    logger.info('Generated INSERT TMP table scripts.')
    logger.info('Generate TMP Table Script completed.')
    return {'RPT_TABLE': rpt_tables}


@util.timeit
def gen_feature_script(response):
    # logger.info('Generating Feature scripts - Preparing')
    
    # RPT_DT, config = response['RPT_DT'], response['config']
    # FS_TBL_NM = config['FEATURE_STORE_TBL']
    # RPT_DT_TBL = response['RPT_DT_TBL']

    # REPORT_DATE_FOLDER = os.path.join(SQL_FEATURE_FOLDER, 'report_date', RPT_DT)
    # PLACEHOLDER_FOLDER = os.path.join(SQL_FEATURE_FOLDER, 'placeholder')
    # STRUCTURED_PLACEHOLDER_FOLDER = os.path.join(PLACEHOLDER_FOLDER, 'structured')
    # UNSTRUCTURED_PLACEHOLDER_FOLDER = os.path.join(PLACEHOLDER_FOLDER, 'unstructured')
    
    # logger.info('REPORT_DATE_FEATURE_FOLDER: ' + REPORT_DATE_FOLDER)

    # # Create prerequisite folder
    # if not os.path.exists(REPORT_DATE_FOLDER):
    #     os.makedirs(REPORT_DATE_FOLDER)
    # for subf in ['unstructured', 'structured']:
    #     subfp = os.path.join(REPORT_DATE_FOLDER, subf)
    #     if not os.path.exists(subfp):
    #         os.makedirs(subfp)
    
    # # Loop through all files in the UNSTRUCTURED placeholder
    # for filename in os.listdir(UNSTRUCTURED_PLACEHOLDER_FOLDER):
    #     if filename.endswith(".sql"):
    #         # Read the file
    #         with open(os.path.join(UNSTRUCTURED_PLACEHOLDER_FOLDER, filename), "r") as file:
    #             file_content = file.read()
    #         # Replace the patterns with the values above
    #         file_content = file_content.replace("{RPT_DT}", RPT_DT)
    #         file_content = file_content.replace("{RPT_DT_TBL}", RPT_DT_TBL)
    #         file_content = file_content.replace("{TBL_NM}", FS_TBL_NM)
    #         # Write the updated content back to the file
    #         filename_only = os.path.splitext(os.path.basename(filename))[0]
    #         table_rpt_dt = filename_only + '_' + RPT_DT_TBL 
    #         filename_dt = table_rpt_dt + '.sql'
    #         with open(os.path.join(REPORT_DATE_FOLDER, 'unstructured', filename_dt), "w") as file:
    #             file.write(file_content)
    #             logger.debug('Created file: ' + os.path.join(RPT_DT, 'unstructured', filename_dt))
    # logger.info('Generated UNSTRUCTURED feature scripts.')
    
    # # Loop through all files in the STRUCTURED placeholder
    # for filename in os.listdir(STRUCTURED_PLACEHOLDER_FOLDER):
    #     if filename.endswith(".sql"):
    #         # Read the file
    #         with open(os.path.join(STRUCTURED_PLACEHOLDER_FOLDER, filename), "r") as file:
    #             file_content = file.read()
    #         # Replace the patterns with the values above
    #         file_content = file_content.replace("{RPT_DT}", RPT_DT)
    #         file_content = file_content.replace("{RPT_DT_TBL}", RPT_DT_TBL)
    #         file_content = file_content.replace("{TBL_NM}", FS_TBL_NM)
    #         # Write the updated content back to the file
    #         filename_only = os.path.splitext(os.path.basename(filename))[0]
    #         table_rpt_dt = filename_only + '_' + RPT_DT_TBL 
    #         filename_dt = table_rpt_dt + '.sql'
    #         with open(os.path.join(REPORT_DATE_FOLDER, 'structured', filename_dt), "w") as file:
    #             file.write(file_content)
    #             logger.debug('Created file: ' + os.path.join(RPT_DT, 'structured', filename_dt))
    # logger.info('Generated STRUCTURED feature scripts.')
    # logger.info('Generate Feature Script completed.')
    return {}


def aggregate_sql_scripts(response):
    # RPT_DT, config = response['RPT_DT'], response['config']
    # RPT_DT_TBL = response['RPT_DT_TBL']
    # REPORT_TBL_CREATE_FOLDER = os.path.join(SQL_TABLE_FOLDER, 'report_date', RPT_DT, 'create')
    # REPORT_TBL_INSERT_FOLDER = os.path.join(SQL_TABLE_FOLDER, 'report_date', RPT_DT, 'insert')
    # REPORT_FTR_UNSTRUCTURED_FOLDER = os.path.join(SQL_FEATURE_FOLDER, 'report_date', RPT_DT, 'unstructured')
    # REPORT_FTR_STRUCTURED_FOLDER = os.path.join(SQL_FEATURE_FOLDER, 'report_date', RPT_DT, 'structured')
    
    # # Aggregate TMP table scripts first
    # # Create TMP table first
    # create_scripts = []
    # for tbl in os.listdir(REPORT_TBL_CREATE_FOLDER):
    #     tbl_fp = os.path.join(REPORT_TBL_CREATE_FOLDER, tbl)
    #     if os.path.isfile(tbl_fp):
    #         with open(tbl_fp, 'r') as f:
    #             content = f.read().strip()
    #             create_scripts.append(content)
    # create_scripts = ';\n\n\nCOMMIT;\n\n\n'.join(create_scripts)
    # logger.debug(f'Create scripts {create_scripts}')
    
    # # Insert TMP table script
    # insert_scripts = []
    # insert_files = os.listdir(REPORT_TBL_INSERT_FOLDER)
    
    # # CINS_TMP_CUSTOMER and CINS_TMP_CARD_DIM first then others
    # cust_tbl = f'CINS_TMP_CUSTOMER_{RPT_DT_TBL}.sql'
    # if cust_tbl in insert_files:
    #     insert_files.remove(cust_tbl)
    # cust_tbl = os.path.join(REPORT_TBL_INSERT_FOLDER, cust_tbl)
    # with open(cust_tbl, 'r') as f:
    #     content = f.read().strip()
    #     insert_scripts.append(content)
    
    # card_tbl = f'CINS_TMP_CARD_DIM_{RPT_DT_TBL}.sql'
    # if card_tbl in insert_files:
    #     insert_files.remove(card_tbl)
    # card_tbl = os.path.join(REPORT_TBL_INSERT_FOLDER, card_tbl)
    # with open(card_tbl, 'r') as f:
    #     content = f.read().strip()
    #     insert_scripts.append(content)

    # for tbl in insert_files:
    #     tbl_fp = os.path.join(REPORT_TBL_INSERT_FOLDER, tbl)
    #     if os.path.isfile(tbl_fp):
    #         with open(tbl_fp, 'r') as f:
    #             content = f.read().strip()
    #             insert_scripts.append(content)
    # insert_scripts = ';\n\n\nCOMMIT;\n\n\n'.join(insert_scripts)
    # logger.debug(f'Insert scripts {insert_scripts}')
    
    # # Aggregate INSERT feature scripts 
    # unstructured_scripts = []
    # for tbl in os.listdir(REPORT_FTR_UNSTRUCTURED_FOLDER):
    #     tbl_fp = os.path.join(REPORT_FTR_UNSTRUCTURED_FOLDER, tbl)
    #     if os.path.isfile(tbl_fp):
    #         with open(tbl_fp, 'r') as f:
    #             content = f.read().strip()
    #             unstructured_scripts.append(content)
    # unstructured_scripts = ';\n\n\nCOMMIT;\n\n\n'.join(unstructured_scripts)
    # logger.debug(f'Unstructured feature scripts {unstructured_scripts}')
    
    # structured_scripts = []
    # for tbl in os.listdir(REPORT_FTR_STRUCTURED_FOLDER):
    #     tbl_fp = os.path.join(REPORT_FTR_STRUCTURED_FOLDER, tbl)
    #     if os.path.isfile(tbl_fp):
    #         with open(tbl_fp, 'r') as f:
    #             content = f.read().strip()
    #             structured_scripts.append(content)
    # structured_scripts = ';\n\n\nCOMMIT;\n\n\n'.join(structured_scripts)
    # logger.debug(f'Structured feature scripts {structured_scripts}')
    
    # agg_scripts = "\n" + create_scripts + ';\n\n\nCOMMIT;\n\n\n'
    # agg_scripts += insert_scripts + ';\n\n\nCOMMIT;\n\n\n'
    # agg_scripts += unstructured_scripts + ';\n\n\nCOMMIT;\n\n\n'
    # agg_scripts += structured_scripts + ';\n\n\nCOMMIT;\n\n\n'
    # agg_scripts = agg_scripts.strip()
    
    # logger.debug(f'Final aggregated scripts {agg_scripts}')
    # agg_fp = f"./output/dev_fs_{response['RPT_DT_TBL']}.sql"
    # with open(agg_fp, 'w') as f:
    #     f.writelines(agg_scripts)
    # logger.info(f'Final [DEV] aggregated scripts saved at {agg_fp}')
    
    
    # # Convert DW_ANALYTICS to DWPROD for production
    # prod_agg_scripts = agg_scripts.replace('DW_ANALYTICS', 'DWPROD')
    # prod_agg_fp = f"./output/prod_fs_{response['RPT_DT_TBL']}.sql"
    # with open(prod_agg_fp, 'w') as f:
    #     f.writelines(prod_agg_scripts)
    # logger.info(f'Final [PROD] aggregated scripts saved at {prod_agg_fp}')
    
    # return {
    #     'DEV_FS': {
    #         'script': agg_scripts,
    #         'filepath': agg_fp
    #     },
    #     'PROD_FS': {
    #         'script': prod_agg_scripts,
    #         'filepath': prod_agg_fp
    #     }
    # }
    return {}