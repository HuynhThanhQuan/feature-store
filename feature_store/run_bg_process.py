import subprocess


command = ["/opt/bitnami/miniconda/bin/python", "main.py", "1"]

subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

print('Job is running on background')