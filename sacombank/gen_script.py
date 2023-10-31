import os
import logging
import datetime
import logging
import util

logger = logging.getLogger(__name__)

@util.timeit
def gen_tmp_table_script(response):
    logger.info('Generating TMP table scripts - Preparing')
    RPT_DT, config = response['RPT_DT'], response['config']
    RPT_DT_TBL = response['RPT_DT_TBL']
    SQL_TABLE_FOLDER = config['SQL_TABLE_FOLDER']
    
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
    FS_TBL_NM = config['FEATURE_STORE_TBL_NM']
    RPT_DT_TBL = response['RPT_DT_TBL']

    SQL_FEATURE_FOLDER = config['SQL_FEATURE_FOLDER']
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
