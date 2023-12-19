import time
from functools import wraps
import logging
import yaml
import re
import os


logger = logging.getLogger(__name__)


COMMIT_CHECKPOINT = '\n\n\nCOMMIT;\n\n\n'


def timeit(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.debug(f'[timeit] Function "{func.__name__}" took "{elapsed_time}" seconds')
        return result
    return wrapper




def extract_desc_yaml_section_from_string(s):
    pattern = r'(/\*(.*?)\*/)'
    match = re.search(pattern, s, re.DOTALL)
    if match:
        comment_sec = match.group(1).strip()
        desc_sec = match.group(2).strip()
        return yaml.safe_load(desc_sec), desc_sec, s.replace(comment_sec, '').strip()
    else:
        return None, None, None
    

def convert_yaml_to_string(yaml_data):
    return yaml.dump(yaml_data, default_flow_style=False, sort_keys=False)


def read_sql_file(fp):
    content = None
    if os.path.exists(fp):
        with open(fp,'r') as f:
            content = f.read().strip()
            if content.endswith(';'):
                id = content.rfind(';')
                content = content[:id]
    return content


def format_sql(text):
    text = text.strip()
    if text.endswith(';'):
        return text
    return text + ';'


def post_processing_sql(query):
    query = query.replace(';;',';')
    return query