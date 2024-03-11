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
conn, cur = oraDB.connect_CINS_SMY()
print('Connected DB - conn')

conn_aly, cur_aly = oraDB.connect_DW_ANALYTICS()
print('Connected oraDW_ANALYTICS - conn_aly')