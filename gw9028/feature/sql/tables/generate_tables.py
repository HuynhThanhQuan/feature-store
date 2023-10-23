import os
import logging

# Set up logging
logging.basicConfig(filename='generate_tables.log', level=logging.DEBUG)

logging.info('Starting script...')



# Assuming RPT_DT is a string in the format "YYYY-MM-DD"
RPT_DT = "01-10-2023" #DD-MM-YYYY
RPT_DT_TBL = RPT_DT.replace("-", "")

# Set the folder path
folder_path = "./"

REPORT_DATE_FOLDER = os.path.join(folder_path, 'report_date', RPT_DT)
PLACEHOLDER_FOLDER = os.path.join(folder_path, 'placeholder')
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
