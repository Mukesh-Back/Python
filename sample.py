import random
import pandas as pd
import datetime as dt
import plotly.express as px

rice_prices = {
    "Basmati_rice": 170,
    "Kurnool_Sona_Masoori": 70, 
    "Red_rice": 125,
    "Mogra_rice": 57,
    "Kolam_rice": 88,
    "Indryani_rice": 140, 
    "Black_rice": 300,
    "Seeraga_samba_rice": 97,
    "Bamboo_rice": 67,
    "Ponni_Rice": 74 
}

user=input("Give the Rice brand :")

start_date = dt.datetime.now()
end_date = start_date + dt.timedelta(days=365*2)
date_range = pd.date_range(start_date, end_date)
prices = {rice: [] for rice in rice_prices}

def weekly(price):
    return price * (1 + (random.uniform(-1, 2) / 100))

def monthly(price):
    return price * 1.005

def yearly(price, year):
    return price * (1 + 0.0025 * (year - start_date.year - 1))

for rice, base_price in rice_prices.items():
    current_price = base_price
    for date in date_range:
        day = date.weekday()
        
        if day == 6: 
            if date.month in [10, 11]:
                weekly_increase = random.uniform(0, 2) / 100
                current_price = current_price * (1 + weekly_increase)
            else:
                current_price = weekly(current_price)
        
        if date.day == 1: 
            current_price = monthly(current_price)
            if date.year > start_date.year:
                current_price = yearly(current_price, date.year)
        
        prices[rice].append((date, current_price))


rows = []
for rice, price_data in prices.items():
    for date, price in price_data:
        rows.append([date.date(), rice, price])

df = pd.DataFrame(rows, columns=["Date", "Rice_Type", "Price"])
df.to_csv("Results.csv",index=False)

print("Store Successfully")

if user in df['Rice_Type'].unique():
    filtered_df = df[df['Rice_Type'] == user]
    fig = px.histogram(filtered_df, x="Date", y="Price", title=f"Histogram of {user} Prices at 2 years")
    fig.show()
else:
    print(f"The rice type '{user}' does not exist in the DataFrame")