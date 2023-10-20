import re

# Assuming RPT_DT is a string in the format "YYYY-MM-DD"
RPT_DT = "2023-10-01"

# Read the file contents
with open("./script_create_tbl.sql", "r") as f:
    file_contents = f.read()

# Replace the placeholder with the input variable
file_contents = re.sub(r"\{RPT_DT\}", RPT_DT, file_contents)

print(file_contents)

# Write the updated contents to a new file
with open(f"./script_create_tbl_{RPT_DT}.sql", "w") as f:
    f.write(file_contents)
