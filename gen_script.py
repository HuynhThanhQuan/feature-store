import os
import logging
import datetime

# Set up logging
now = datetime.datetime.now()
logging.basicConfig(level=logging.DEBUG)
logging.info('Log create tmp table script started at ' + now.strftime('%Y-%m-%d %H:%M:%S'))


def gen_tmp_table_script(RPT_DT, config):
    # Assuming RPT_DT is a string in the format "DD-MM-YYYY"
    RPT_DT_TBL = RPT_DT.replace("-", "")

    SQL_TABLE_FOLDER = config['SQL_TABLE_FOLDER']
    REPORT_DATE_FOLDER = os.path.join(SQL_TABLE_FOLDER, 'report_date', RPT_DT)
    PLACEHOLDER_FOLDER = os.path.join(SQL_TABLE_FOLDER, 'placeholder')
    
    logging.info('REPORT_DATE_FOLDER: ' + REPORT_DATE_FOLDER)
    logging.info('PLACEHOLDER_FOLDER: ' + PLACEHOLDER_FOLDER)

    if not os.path.exists(REPORT_DATE_FOLDER):
        os.makedirs(REPORT_DATE_FOLDER)

    # Loop through all files in the folder
    for filename in os.listdir(PLACEHOLDER_FOLDER):
        if filename.endswith(".sql"):
            # Read the file
            with open(os.path.join(PLACEHOLDER_FOLDER, filename), "r") as file:
                file_content = file.read()
            # Replace the patterns with the values above
            file_content = file_content.replace("{RPT_DT}", RPT_DT)
            file_content = file_content.replace("{RPT_DT_TBL}", RPT_DT_TBL)
            # Write the updated content back to the file
            filename_only = os.path.splitext(os.path.basename(filename))[0]
            filename_dt = filename_only + '_' + RPT_DT_TBL + '.sql'
            with open(os.path.join(REPORT_DATE_FOLDER, filename_dt), "w") as file:
                file.write(file_content)
                logging.info('Created file: ' + filename_dt)
    logging.info('Script completed.')
