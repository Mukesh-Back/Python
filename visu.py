import matplotlib.pyplot as plt
import pandas as pd

data=pd.read_csv("c:\\Users\\visualapp\\Music\\Results\\ml_ready_data.csv",index_col=0)
data = data.apply(pd.to_numeric, errors='coerce')

plt.boxplot(data)
plt.show()

