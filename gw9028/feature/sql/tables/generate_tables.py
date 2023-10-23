import os

# Assuming RPT_DT is a string in the format "YYYY-MM-DD"
RPT_DT = "2023-10-01"
RPT_DT_TBL = RPT_DT.replace("-", "")

# Set the folder path
folder_path = "./"

REPORT_DATE_FOLDER = os.path.join(folder_path, 'report_date', RPT_DT)
PLACEHOLDER_FOLDER = os.path.join(folder_path, 'placeholder')

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
        with open(os.path.join(REPORT_DATE_FOLDER, filename), "w") as file:
            file.write(file_content)
