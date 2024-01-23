from matplotlib import style
import matplotlib.pyplot as plt
print(plt.style.available)

plt.style.use('bmh')

from datetime import datetime
import pandas as pd
import seaborn as sns
import numpy as np

import sys
sys.path.append('../../..')
print('Add path ../../..')

from oraDB import oraDB
conn, cur = oraDB.connect()
print('Connected DB - conn')

from oraDW_ANALYTICS import oraDW_ANALYTICS
conn_aly, cur_aly = oraDW_ANALYTICS.connect()
print('Connected oraDW_ANALYTICS - conn_aly')