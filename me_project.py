import random
import pandas as pd
from datetime import datetime, timedelta
from neo4j import GraphDatabase
import random
import pandas as pd
from datetime import datetime, timedelta


class Neo4jConnection:
    def __init__(self, uri, user, password):
        try:
            self._driver = GraphDatabase.driver(uri, auth=(user, password))
        except Exception as e:
            raise ConnectionError(f"Error connecting to Neo4j: {e}")

    def close(self):
        if self._driver:
            self._driver.close()

    def execute_query(self, query, parameters=None):
        try:
            with self._driver.session() as session:
                return session.run(query, parameters).data()
        except Exception as e:
            print(f"Error executing query: {e}")
            return []


def generate_sowing_stage_dataset():
    try:
        # Input date range from user
        start_date_input = input("Enter the start date (YYYY-MM-DD): ")
        end_date_input = input("Enter the end date (YYYY-MM-DD): ")

        start_date = datetime.strptime(start_date_input, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_input, "%Y-%m-%d")

        if start_date > end_date:
            raise ValueError("Start date must be earlier than end date.")

        # Define sowing parameters with key-value ranges
        Tillering_parameters = {
            "Number of Tillers per Plant":(3,6),
            "Tillering Duration":(25,45),
            "Tillering Rate":(2,5),
            "Tillering Density":(20,40),
            "Optimal Nitrogen Uptake":(60,120),
            "Tillering Phase Duration":(15,30),
            "Number of Panicles per Plant":(10,20),
            "Number of Grains per Panicle":(100,300),
            "Spikelet Fertility":(70,90),
            "1000-Grain Weight":(20,35),
            "Grain Filling Duration":(20,45),
            "Grain Filling Rate":(3,6),
            "Soil Moisture Content":(50,80),
            
            "Solar Radiation Exposure Range":(400 ,600),
            "UV Radiation Exposure Range":(0.1,1.5),
            "Infrared Radiation Exposure Range":(200,500),
            "Solar Radiation Per Day":(34.56,51.84),
            "UV Radiation Per Day":(8.64,129.6),
            "Infrared Radiation Per Day":(17.28,43.2),

            "Brown Planthopper":(20,30),
            "White-backed Planthopper":(20,30),
            "Rice Stem Borer":(15,25),
            "Rice Leaf Folder":(5,10),
            "Cutworm":(10,15),
            "Rice Gall Midge":(10,20),
            "Tungro Virus Vector (Green Leafhopper)":(15,30),
            "Plant Bugs":(5,10),
            "Root Knot Nematode":(10,20),
            "Chemical Insecticides":(0.5,1.0),
            "Biological Insecticides":(0.5,1.0),
            "Systemic Insecticides":(0.2,0.5),
            "Contact Insecticides":(1.0,2.0),
            "Organic/Inorganic Insecticides":(1.0,3.0),
            
            "Auxins":(0.1,10),
            "Cytokinins":(10,50),
            "Gibberellins":(50,200),
            "Abscisic Acid (ABA)":(0.1,10),
            "Ethylene":(100,500),
            "Brassinosteroids":(0.01,1.0),
            "Contact Herbicides":(0.2,0.8),
            "Systemic Herbicides":(0.4,1.2),
            "Selective Herbicides":(0.2,0.8),
            "Non-Selective Herbicides":(0.4,1.2),
            "Pre-emergence Herbicides":(0.4,0.8),
            "Post-emergence Herbicides":(0.2,0.6),
            "Graminicides":(0.2,0.6),
            "Broadleaf Herbicides":(0.2,0.4),
            "Aquatic Herbicides":(0.8,1.6),
            "Glyphosate-based Herbicides":(0.4,0.8),
            "Phenoxy Herbicides":(0.2,0.4),
            "Triazines"	:(0.4,0.8),
            "Aryloxyphenoxypropionates"	:(0.2,0.4),
            "Carbamates":(0.2,0.8),
            
            "Temperature":(25,30),
            "Water Availability":(3.24,3.65 ),
            "Soil pH":(5.5,7.0),
            "Potassium":(16.1,24.2),
            "Light Intensity":(12,14),
            "Humidity":(70,82),
            "Wind Speed":(5,10),
            "Soil Salinity":(1,2),
            "Oxygen Levels in Soil":(10,20),
            "Nitrogen":(20.2,32.4),
            "Phosphorus":(12.1,16.1),
            "Loamy Soil":(30,40),
            "Clay Loam":(20,30),
            "Sandy Loam":(50,70),
            "Clay Soil":(10,20),
            "Silt Loam":(20,30),
            "Stem Borers":(15,25),
            "Leafhoppers/Planthoppers":(20,30),
            "Aphids":(10,20),
            "Sheath Blight":(10,20),
            "Blast Disease":(10,30),
            "Rice Tungro Virus":(15,30),
            "Xylem Transport":(25,30),
	        "Phloem Transport Nitrogen":(20.2,32.4),
	        "Phloem Transport Phosphorus":(12.1,16.1),
	        "Phloem Transport Potassium":(16.1,24.2),
	        "Lateral Root Development":(10,20),
            "Cytokinin Receptors light intensity":(12,14),
            "Auxin Receptors":(5.5,7.0),
            "Gibberellin Receptors":(25,30),
            "Abscisic Acid (ABA) Receptors":(70,80),
            "Light Receptors (Phytochromes)":(12,14),
            "Heat Stress Receptors":(1,10),
            "Rice Tungro Virus (RTV) Temperature":(25,30),
            "Rice Tungro Virus (RTV) Humidity":(70,80),
            "Rice Tungro Virus (RTV) Water Availability:":(3.24,3.65),
            "Rice Stripe Virus (RSV) Temperature":(25,30),
            "Rice Stripe Virus (RSV) Humidity":(70,80),
            "Rice Stripe Virus (RSV) Water Availability":(3.24,3.65),
            "Loamy Soil":(6.0,7.0),
            "Clay Loam":(6.0,7.5),
            "Silty Clay":(5.5,7.0),
            "Sandy Loam":(6.0,7.5),
            "Peaty Soil":(5.5,6.5),
            "Saline Soil":(7.0,8.5),
            "Alkaline Soil":(7.5,8.5),
            "Phosphate":(12.1,16.1),
            "Sodium":(405,607),
            "Nitrogen":(20.2,32.4),
            "Magnesium":(5,10),
            "Potassium":(16.1,24.2),
            "Vegetation Index":(-1,1),
            "Crop Phenology":(30,60),
            "Leaf Area Index (LAI)":(2.5,3.5),
            "Chlorophyll Content":(35,45),
            "Water Stress":(20,50),
            "Soil Organic Matter":(1,3),
            "Plant Health & Disease":(0,30),
            "Cloud Cover":(40,70),
            "Precipitation":(0,50),
            "Temperature":(25,32),
            "Boron":(0.2,1.0),
            "Chlorine":(10,100),
            "Copper":(0.1,0.5),
            "Iron":(4.5,10),
            "Manganese":(1,5),
            "Molybdenum":(0.01,0.1),
            "Zinc":(1,3),
            "Tebuconazole":	(0.05,0.15),
            "Propiconazole":(0.02,0.10),
            "Difenoconazole":(0.01,0.05),
            "Cyproconazole":(0.03,0.12),
            "Epoxiconazole":(0.01,0.04 ),
            "Hexaconazole":(0.02,0.08 ),
            "Myclobutanil":(0.01,0.03 ),
            "Flutriafol":(0.02,0.10 ),
            "Metconazole":(0.02,0.06 ),
            "Moisture":(20,30),
            "Ambient Temperature":(25,32),
            "Radiation Exposure":(400,700),
            "Water Management":(75,90),
            "Soil Fertility Management Nitrogen":(20,60),
            "Soil Fertility Management Phosphorus":(10,40),
            "Soil Fertility Management Potassium":(30,50),            
            "Pest and Disease Control":(1,3),
            "Crop Rotation":(1,3),
            "Nutrient Management Nitrogen":(50,100),
            "Nutrient  Management Phosphorus":(10,30),
            "Nutrient  Management Potassium":(20,60),            
            "Irrigation Management":(500,1000),
            "Harvest Management":(30,40),
            "Post-harvest Handling":(18,22)

        }

        # Additional new parameters
        new_parameters = {
            "Plant_Vigor": (0, 100),
            "Plant_Stress_Resilience": (0, 100),
            "Land_Preparation": (1, 10),  # Scale 1 to 10
            "Biological_Interactions": (0, 100),
            "Pollution": (0, 50),  # Pollution level
        }

        # Initial crop details
        crop_variety = random.choice(["Long", "Medium", "Short"])
        season = random.choice(["Rabi", "Kharif"])
        yield_trait = random.choice(["High Yield", "Medium Yield", "Hybrid"])

        # Prepare data container
        data = []
        current_date = start_date
        total_days = (end_date - start_date).days + 1
        times_of_day = ["morning", "afternoon", "evening"]

        # Variables for harvesting logic
        accumulated_yield = 0
        days_elapsed = 0
        cycle_days = random.randint(120, 140)
        day_of_sowing = 0
        # Generate data day by day
        for day in range(total_days):
            # Sowing stage parameters (always "Sowing")
            crop_stage = "Sowing"

            # Add a row of data for each time of day
            for time_of_day in times_of_day:
                row = {
                    "Date": current_date.strftime("%d/%m/%Y"),
                    "TimeOfDay": time_of_day,
                    "Crop_Stage": crop_stage,
                    "Rice_Crop_Variety": crop_variety,
                    "Season": season,
                    "Yield_Trait": yield_trait,
                    "Duration": random.randint(90, 140),
                    "Day_of_Sowing": day_of_sowing,
                }

                # Add sowing parameters with random values within their ranges
                for param, (min_val, max_val) in {
                    **Tillering_parameters,
                    **new_parameters,
                }.items():
                    if isinstance(min_val, float):
                        row[param] = round(random.uniform(min_val, max_val), 2)
                    else:
                        row[param] = random.randint(min_val, max_val)

                # Calculate yield range (0 to 100) based on the key parameters
                yield_range = calculate_yield_range(row, Tillering_parameters)
                row["Yield_Range"] = yield_range

                # Accumulate yield for the harvesting logic
                accumulated_yield += yield_range

                data.append(row)

            # Increment the date and track elapsed days
            current_date += timedelta(days=1)
            days_elapsed += 1
            day_of_sowing = (day_of_sowing + 1) % 8
            # Check if the current cycle is complete
            if days_elapsed >= cycle_days:

                # Update crop details for the next cycle
                crop_variety = random.choice(["Long", "Medium", "Short"])
                season = "Kharif" if season == "Rabi" else "Rabi"
                yield_trait = random.choice(["High Yield", "Medium Yield", "Hybrid"])

                # Reset variables for the next cycle
                accumulated_yield = 0
                days_elapsed = 0
                cycle_days = random.randint(120, 140)

        return pd.DataFrame(data)

    except Exception as e:
        print(f"Error generating dataset: {e}")
        return pd.DataFrame()
