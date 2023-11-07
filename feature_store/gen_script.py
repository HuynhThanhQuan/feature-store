import os
import logging
import datetime
import logging
import util
from pathlib import Path

logger = logging.getLogger(__name__)


SQL_TABLE_FOLDER = './sql/table'
SQL_FEATURE_FOLDER = './sql/feature'


@util.timeit
def gen_tmp_table_script(response):
    logger.info('Generating TMP table scripts - Preparing')
    RPT_DT, config = response['RPT_DT'], response['config']
    RPT_DT_TBL = response['RPT_DT_TBL']
    
    REPORT_DATE_FOLDER = os.path.join(SQL_TABLE_FOLDER, 'report_date', RPT_DT)
    PLACEHOLDER_FOLDER = os.path.join(SQL_TABLE_FOLDER, 'placeholder')
    CREATE_PLACEHOLDER_FOLDER = os.path.join(PLACEHOLDER_FOLDER, 'create')
    INSERT_PLACEHOLDER_FOLDER = os.path.join(PLACEHOLDER_FOLDER, 'insert')
    
    logger.info('REPORT_DATE_FOLDER: ' + REPORT_DATE_FOLDER)

    # Create prerequisite folder
    if not os.path.exists(REPORT_DATE_FOLDER):
        os.makedirs(REPORT_DATE_FOLDER)
    for subf in ['create', 'insert']:
        subfp = os.path.join(REPORT_DATE_FOLDER, subf)
        if not os.path.exists(subfp):
            os.makedirs(subfp)
    
    rpt_tables = []
    # Loop through all files in the CREATE placeholder
    for filename in os.listdir(CREATE_PLACEHOLDER_FOLDER):
        if filename.endswith(".sql"):
            # Read the file
            with open(os.path.join(CREATE_PLACEHOLDER_FOLDER, filename), "r") as file:
                file_content = file.read()
            # Replace the patterns with the values above
            file_content = file_content.replace("{RPT_DT}", RPT_DT)
            file_content = file_content.replace("{RPT_DT_TBL}", RPT_DT_TBL)
            # Write the updated content back to the file
            filename_only = os.path.splitext(os.path.basename(filename))[0]
            table_rpt_dt = filename_only + '_' + RPT_DT_TBL 
            filename_dt = table_rpt_dt + '.sql'
            rpt_tables.append(table_rpt_dt)
            with open(os.path.join(REPORT_DATE_FOLDER, 'create', filename_dt), "w") as file:
                file.write(file_content)
                logger.debug('Created file: ' + os.path.join(RPT_DT, 'create', filename_dt))
    logger.info('Generated CREATE TMP table scripts.')
    
    # Loop through all files in the INSERT placeholder
    for filename in os.listdir(INSERT_PLACEHOLDER_FOLDER):
        if filename.endswith(".sql"):
            # Read the file
            with open(os.path.join(INSERT_PLACEHOLDER_FOLDER, filename), "r") as file:
                file_content = file.read()
            # Replace the patterns with the values above
            file_content = file_content.replace("{RPT_DT}", RPT_DT)
            file_content = file_content.replace("{RPT_DT_TBL}", RPT_DT_TBL)
            # Write the updated content back to the file
            filename_only = os.path.splitext(os.path.basename(filename))[0]
            table_rpt_dt = filename_only + '_' + RPT_DT_TBL 
            filename_dt = table_rpt_dt + '.sql'
            with open(os.path.join(REPORT_DATE_FOLDER, 'insert', filename_dt), "w") as file:
                file.write(file_content)
                logger.debug('Created file: ' + os.path.join(RPT_DT, 'insert', filename_dt))
                
    logger.info('Generated INSERT TMP table scripts.')
    logger.info('Generate TMP Table Script completed.')
    return {'RPT_TABLE': rpt_tables}

@util.timeit
def gen_feature_script(response):
    logger.info('Generating Feature scripts - Preparing')
    
    RPT_DT, config = response['RPT_DT'], response['config']
    FS_TBL_NM = config['FEATURE_STORE_TBL']
    RPT_DT_TBL = response['RPT_DT_TBL']

    REPORT_DATE_FOLDER = os.path.join(SQL_FEATURE_FOLDER, 'report_date', RPT_DT)
    PLACEHOLDER_FOLDER = os.path.join(SQL_FEATURE_FOLDER, 'placeholder')
    STRUCTURED_PLACEHOLDER_FOLDER = os.path.join(PLACEHOLDER_FOLDER, 'structured')
    UNSTRUCTURED_PLACEHOLDER_FOLDER = os.path.join(PLACEHOLDER_FOLDER, 'unstructured')
    
    logger.info('REPORT_DATE_FEATURE_FOLDER: ' + REPORT_DATE_FOLDER)

    # Create prerequisite folder
    if not os.path.exists(REPORT_DATE_FOLDER):
        os.makedirs(REPORT_DATE_FOLDER)
    for subf in ['unstructured', 'structured']:
        subfp = os.path.join(REPORT_DATE_FOLDER, subf)
        if not os.path.exists(subfp):
            os.makedirs(subfp)
    
    # Loop through all files in the UNSTRUCTURED placeholder
    for filename in os.listdir(UNSTRUCTURED_PLACEHOLDER_FOLDER):
        if filename.endswith(".sql"):
            # Read the file
            with open(os.path.join(UNSTRUCTURED_PLACEHOLDER_FOLDER, filename), "r") as file:
                file_content = file.read()
            # Replace the patterns with the values above
            file_content = file_content.replace("{RPT_DT}", RPT_DT)
            file_content = file_content.replace("{RPT_DT_TBL}", RPT_DT_TBL)
            file_content = file_content.replace("{TBL_NM}", FS_TBL_NM)
            # Write the updated content back to the file
            filename_only = os.path.splitext(os.path.basename(filename))[0]
            table_rpt_dt = filename_only + '_' + RPT_DT_TBL 
            filename_dt = table_rpt_dt + '.sql'
            with open(os.path.join(REPORT_DATE_FOLDER, 'unstructured', filename_dt), "w") as file:
                file.write(file_content)
                logger.debug('Created file: ' + os.path.join(RPT_DT, 'unstructured', filename_dt))
    logger.info('Generated UNSTRUCTURED feature scripts.')
    
    # Loop through all files in the STRUCTURED placeholder
    for filename in os.listdir(STRUCTURED_PLACEHOLDER_FOLDER):
        if filename.endswith(".sql"):
            # Read the file
            with open(os.path.join(STRUCTURED_PLACEHOLDER_FOLDER, filename), "r") as file:
                file_content = file.read()
            # Replace the patterns with the values above
            file_content = file_content.replace("{RPT_DT}", RPT_DT)
            file_content = file_content.replace("{RPT_DT_TBL}", RPT_DT_TBL)
            file_content = file_content.replace("{TBL_NM}", FS_TBL_NM)
            # Write the updated content back to the file
            filename_only = os.path.splitext(os.path.basename(filename))[0]
            table_rpt_dt = filename_only + '_' + RPT_DT_TBL 
            filename_dt = table_rpt_dt + '.sql'
            with open(os.path.join(REPORT_DATE_FOLDER, 'structured', filename_dt), "w") as file:
                file.write(file_content)
                logger.debug('Created file: ' + os.path.join(RPT_DT, 'structured', filename_dt))
    logger.info('Generated STRUCTURED feature scripts.')
    logger.info('Generate Feature Script completed.')
    return {}


def aggregate_sql_scripts(response):
    RPT_DT, config = response['RPT_DT'], response['config']
    RPT_DT_TBL = response['RPT_DT_TBL']
    REPORT_TBL_CREATE_FOLDER = os.path.join(SQL_TABLE_FOLDER, 'report_date', RPT_DT, 'create')
    REPORT_TBL_INSERT_FOLDER = os.path.join(SQL_TABLE_FOLDER, 'report_date', RPT_DT, 'insert')
    REPORT_FTR_UNSTRUCTURED_FOLDER = os.path.join(SQL_FEATURE_FOLDER, 'report_date', RPT_DT, 'unstructured')
    REPORT_FTR_STRUCTURED_FOLDER = os.path.join(SQL_FEATURE_FOLDER, 'report_date', RPT_DT, 'structured')
    
    # Aggregate TMP table scripts first
    # Create TMP table first
    create_scripts = []
    for tbl in os.listdir(REPORT_TBL_CREATE_FOLDER):
        tbl_fp = os.path.join(REPORT_TBL_CREATE_FOLDER, tbl)
        if os.path.isfile(tbl_fp):
            with open(tbl_fp, 'r') as f:
                content = f.read().strip()
                create_scripts.append(content)
    create_scripts = ';\n\n\n'.join(create_scripts)
    logger.debug(f'Create scripts {create_scripts}')
    
    # Insert TMP table script
    insert_scripts = []
    insert_files = os.listdir(REPORT_TBL_INSERT_FOLDER)
    
    # CINS_TMP_CUSTOMER and CINS_TMP_CARD_DIM first then others
    cust_tbl = f'CINS_TMP_CUSTOMER_{RPT_DT_TBL}.sql'
    if cust_tbl in insert_files:
        insert_files.remove(cust_tbl)
    cust_tbl = os.path.join(REPORT_TBL_INSERT_FOLDER, cust_tbl)
    with open(cust_tbl, 'r') as f:
        content = f.read().strip()
        insert_scripts.append(content)
    
    card_tbl = f'CINS_TMP_CARD_DIM_{RPT_DT_TBL}.sql'
    if card_tbl in insert_files:
        insert_files.remove(card_tbl)
    card_tbl = os.path.join(REPORT_TBL_INSERT_FOLDER, card_tbl)
    with open(card_tbl, 'r') as f:
        content = f.read().strip()
        insert_scripts.append(content)

    for tbl in insert_files:
        tbl_fp = os.path.join(REPORT_TBL_INSERT_FOLDER, tbl)
        if os.path.isfile(tbl_fp):
            with open(tbl_fp, 'r') as f:
                content = f.read().strip()
                insert_scripts.append(content)
    insert_scripts = ';\n\n\n'.join(insert_scripts)
    logger.debug(f'Insert scripts {insert_scripts}')
    
    # Aggregate INSERT feature scripts 
    unstructured_scripts = []
    for tbl in os.listdir(REPORT_FTR_UNSTRUCTURED_FOLDER):
        tbl_fp = os.path.join(REPORT_FTR_UNSTRUCTURED_FOLDER, tbl)
        if os.path.isfile(tbl_fp):
            with open(tbl_fp, 'r') as f:
                content = f.read().strip()
                unstructured_scripts.append(content)
    unstructured_scripts = ';\n\n\n'.join(unstructured_scripts)
    logger.debug(f'Unstructured feature scripts {unstructured_scripts}')
    
    structured_scripts = []
    for tbl in os.listdir(REPORT_FTR_STRUCTURED_FOLDER):
        tbl_fp = os.path.join(REPORT_FTR_STRUCTURED_FOLDER, tbl)
        if os.path.isfile(tbl_fp):
            with open(tbl_fp, 'r') as f:
                content = f.read().strip()
                structured_scripts.append(content)
    structured_scripts = ';\n\n\n'.join(structured_scripts)
    logger.debug(f'Structured feature scripts {structured_scripts}')
    
    agg_scripts = "\n" + create_scripts + ";\n\n\n" 
    agg_scripts += insert_scripts + ";\n\n\n" 
    agg_scripts += unstructured_scripts + "\n\n\n" 
    agg_scripts += structured_scripts
    agg_scripts = agg_scripts.strip()
    
    logger.debug(f'Final aggregated scripts {agg_scripts}')
    agg_fp = f"./output/dev_fs_{response['RPT_DT_TBL']}.sql"
    with open(agg_fp, 'w') as f:
        f.writelines(agg_scripts)
    logger.info(f'Final [DEV] aggregated scripts saved at {agg_fp}')
    
    
    # Convert DW_ANALYTICS to DWPROD for production
    prod_agg_scripts = agg_scripts.replace('DW_ANALYTICS', 'DWPROD')
    prod_agg_fp = f"./output/prod_fs_{response['RPT_DT_TBL']}.sql"
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