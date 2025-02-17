import pandas as pd
df1=pd.read_csv("C:\\Users\\visualapp\\Downloads\\Agriculture_Data1.csv")
df2=pd.read_csv("C:\\Users\\visualapp\\Downloads\\Agriculture_Data2.csv")
df3 = pd.concat([df1, df2],ignore_index = True,sort = False)
df3['State Code'] = df3["State Code"].fillna(df3['State Code'].median())
df3=df3.drop_duplicates()
df3['Year'] = df3["Year"].fillna(df3['Year'].median())
df3["State Name"]=df3["State Name"].fillna(df3['State Name'].value_counts().idxmax())
df3["Dist Code"]=df3["Dist Code"].fillna(df3["Dist Code"].mean())
df3["Dist Name"]=df3["Dist Name"].fillna(df3['Dist Name'].value_counts().idxmax())

print(df3)
print(dir(pd))