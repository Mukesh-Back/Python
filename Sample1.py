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
start_date = dt.datetime.now()
user_rice = input("Give the Rice brand: ")

print()
Choice =int(input("""Press 1 For Next Day 
Press 2 For Next Week 
Press 3 For Next Month 
Press 4 For Next Year 
                  
Enter the Value: """))

if Choice==1:
    end_date_input = start_date + pd.Timedelta(days=1)

elif Choice==2:
    end_date_input =start_date + pd.Timedelta(weeks=1)

elif Choice==3:
    end_date_input = start_date + pd.DateOffset(months=1)

elif Choice ==4:
    end_date_input = start_date + pd.DateOffset(years=1)


end_date = pd.to_datetime(end_date_input)
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
df.to_csv("Results.csv", index=False)

print("Data stored successfully.")

if user_rice in df['Rice_Type'].unique():
    selected = df[df['Rice_Type'] == user_rice]
    fig = px.line(selected, x="Date", y="Price", title=f"Price for {user_rice} from {start_date} to {end_date}", markers=True)
    fig.show()
else:
    print(f"The rice type '{user_rice}' does not exist in the DataFrame.")