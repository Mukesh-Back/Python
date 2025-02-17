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

        start_date = datetime.strptime(start_date_input, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_input, '%Y-%m-%d')

        if start_date > end_date:
            raise ValueError("Start date must be earlier than end date.")

        # Define sowing parameters with key-value ranges (min, max).
        # Feel free to adjust numeric ranges as appropriate for your actual data.
        sowing_parameters = {
            "soil_pH": (5.5, 7.0),
            "Iron": (4, 10),
            "Manganese": (10, 50),
            "Zinc": (0.5, 2.0),
            "Copper": (0.2, 0.5),
            "Boron": (0.2, 1.0),
            "Molybdenum": (0.01, 0.05),
            "Chlorine": (0.2, 2.0),
            "Nickel": (0.01, 0.1),
            "Potassium": (15, 20),
            "Sodium": (10, 40),
            "Nitrogen": (15, 20),
            "Calcium": (15, 30),
            "Sulphur": (15, 30),
            "Phosphorous": (2, 3),
            "Soil_Moister_Content": (70,90),
            "Vegetarian_Index": (0.3, 0.9),
            "Relative_Humidity": (60, 90),  
            "Temperature": (20, 35),
            "Water": (2.5, 5),
            "Radiation": (300, 600),

            # Keep any original parameters you still want
            "Magnesium": (20, 60),
            "Disease_Resistance": (0, 100),
            "Pest_Resistance": (0, 100),
            "Drought_Tolerance": (0, 100),
            "Heat_Tolerance": (0, 100),
            "Cold_Tolerance": (0, 100),
            # etc.
        }

        # Additional new parameters (if desired)
        new_parameters = {
            "Plant_Vigor": (0, 100),
            "Plant_Stress_Resilience": (0, 100),
            "Land_Preparation": (1, 10),
            "Biological_Interactions": (0, 100),
            "Pollution": (0, 50)
        }

        # Initial crop details
        crop_variety = random.choice(['Long', 'Medium', 'Short'])
        season = random.choice(["Rabi", "Kharif"])
        yield_trait = random.choice(["High Yield", "Medium Yield", "Hybrid"])

        # Prepare data container
        data = []
        current_date = start_date
        total_days = (end_date - start_date).days + 1
        times_of_day = ['morning', 'afternoon', 'evening']

        # Variables for harvesting logic
        accumulated_yield = 0  # Not currently used
        days_elapsed = 0
        cycle_days = random.randint(120, 140)
        day_of_sowing = 0

        # Generate data day by day
        for _ in range(total_days):
            # Sowing stage is always "Sowing"
            crop_stage = "Sowing"

            # Add a row of data for each time of day
            for time_of_day in times_of_day:
                row = {
                    'Crop_Stage': crop_stage,
                    'Date': current_date.strftime('%d/%m/%Y'),  
                    'TimeOfDay': time_of_day,
                    'Rice_Crop_Variety': crop_variety,
                    "Season": season,
                    "Yield_Trait": yield_trait,
                    "Duration": random.randint(90, 140),
                    "Day_of_Sowing": day_of_sowing,     # NO
                    "Yield": round(random.uniform(0, 100), 2),
                }

                # Randomize sowing parameters with new + old
                for param, (min_val, max_val) in {**sowing_parameters, **new_parameters}.items():
                    if isinstance(min_val, float) or isinstance(max_val, float):
                        row[param] = round(random.uniform(min_val, max_val), 2)
                    else:
                        row[param] = random.randint(min_val, max_val)

                # Append the row to our data list
                data.append(row)

            # Increment the date and track elapsed days
            current_date += timedelta(days=1)
            days_elapsed += 1
            day_of_sowing = (day_of_sowing + 1) % 8

            # Check if the current cycle is complete
            if days_elapsed >= cycle_days:
                # Update crop details for the next cycle
                crop_variety = random.choice(['Long', 'Medium', 'Short'])
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


def merge_power_data(sowing_df, power_df):
    """
    Merge the sowing stage data with POWER regional monthly data, 
    ensuring that only specific month and year data is extracted, and include LAT and LON.
    """
    try:
        # Add Year and Month to sowing data
        sowing_df["Year"] = pd.to_datetime(sowing_df["Date"], format="%d/%m/%Y").dt.year
        sowing_df["Month"] = pd.to_datetime(sowing_df["Date"], format="%d/%m/%Y").dt.strftime("%b").str.upper()

        month_columns = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
        parameters_to_extract = ["TS", "T2M", "RH2M", "WS2M", "T2MDEW", "GWETTOP", "CLOUD_AMT", "PRECTOTCORR"]

        # Pivot the POWER data
        power_df_pivoted = power_df.pivot(index=["YEAR", "LAT", "LON"], columns="PARAMETER", values=month_columns)

        # Flatten the column hierarchy
        power_df_pivoted.columns = [f"{param}_{month}" for month, param in power_df_pivoted.columns]
        power_df_pivoted.reset_index(inplace=True)

        final_merged_df = pd.DataFrame()

        # Iterate over sowing data and merge with relevant power data
        for _, sowing_row in sowing_df.iterrows():
            year = sowing_row["Year"]
            month = sowing_row["Month"]

            # Filter power data for the specific year
            power_filtered = power_df_pivoted[
                (power_df_pivoted["YEAR"] == year)
            ]

            # Find the required columns for the month
            relevant_columns = [f"{param}_{month}" for param in parameters_to_extract]

            # Check for missing columns
            missing_columns = [col for col in relevant_columns if col not in power_filtered.columns]
            if missing_columns:
                print(f"Missing columns for {year} {month}: {missing_columns}")
                continue

            # Add LAT and LON to the merged dataset
            power_data = power_filtered[["LAT", "LON"] + relevant_columns]

            # Rename columns for consistency
            power_data.columns = ["LAT", "LON"] + parameters_to_extract

            # Combine sowing data with the first matching row of power data
            combined_row = pd.concat([sowing_row, power_data.iloc[0]], axis=0)
            final_merged_df = pd.concat([final_merged_df, combined_row.to_frame().T], ignore_index=True)

        # Drop temporary columns
        final_merged_df = final_merged_df.drop(columns=["Month", "Year"], errors="ignore")

        return final_merged_df
    except Exception as e:
        print(f"Error in merging data: {e}")
        return pd.DataFrame()

        # return round(yield_score, 2)
    except Exception as e:
        print(f"Error calculating yield range: {e}")
        return 0


Sowing_df = generate_sowing_stage_dataset()
power_df = pd.read_csv("power.csv")  
merged_df = merge_power_data(Sowing_df, power_df)

desired_columns = [
    "Date",
    "TimeOfDay",
    "LAT",
    "LON",
    "TS",
    "T2M",
    "RH2M",
    "WS2M",
    "T2MDEW",
    "GWETTOP",
    "CLOUD_AMT",
    "PRECTOTCORR",

    "Crop_Stage",
    "Rice_Crop_Variety",
    "Season",
    "Duration",
    "Day_of_Sowing",  
    "soil_pH",
    "Iron",
    "Manganese",
    "Zinc",
    "Copper",
    "Boron",
    "Molybdenum",
    "Chlorine",
    "Nickel",
    "Potassium",
    "Sodium",
    "Nitrogen",
    "Calcium",
    "Sulphur",
    "Phosphorous",
    "Yield_Trait",
    "Soil_Moister_Content",
    "Vegetarian_Index",
    "Relative_Humidity",  
    "Temperature",
    "Water",
    "Radiation",

    # Additional parameters
    "Magnesium",
    "Disease_Resistance",
    "Pest_Resistance",
    "Drought_Tolerance",
    "Heat_Tolerance",
    "Cold_Tolerance",
    "Plant_Vigor",
    "Plant_Stress_Resilience",
    "Land_Preparation",
    "Biological_Interactions",
    "Pollution",
    "Yield",
]

merged_df = merged_df.reindex(columns=desired_columns, fill_value=None)

# merged_df.to_csv("sowing_stage_dataset_last.csv", index=False)


def insert_data_into_neo4j(merged_df, neo4j_connection):
    """
    Insert data from merged_df into Neo4j, building the chain of nodes as in your original script.
    """
    try:
        # Ensure that the :Crop node named "Rice" exists
        neo4j_connection.execute_query("MERGE (rice:Crop {name: 'Rice'})")

        for _, row in merged_df.iterrows():
            query = """
            MATCH (rice:Crop {name: 'Rice'})

            CREATE (dateNode:Date {name: 'Date', value: $Date})
            CREATE (timeNode:TimeOfDay {name: 'TimeOfDay', value: $TimeOfDay})
            CREATE (latNode:LAT {name: 'LAT', value: $LAT})
            CREATE (lonNode:LON {name: 'LON', value: $LON})
            CREATE (stage:CropStage {name: 'Crop_Stage', value: $Crop_Stage})
            CREATE (tsNode:TS {name: 'TS', value: $TS})
            CREATE (t2mNode:T2M {name: 'T2M', value: $T2M})
            CREATE (rh2mNode:RH2M {name: 'RH2M', value: $RH2M})
            CREATE (ws2mNode:WS2M {name: 'WS2M', value: $WS2M})
            CREATE (t2mdewNode:T2MDEW {name: 'T2MDEW', value: $T2MDEW})
            CREATE (gwetNode:GWETTOP {name: 'GWETTOP', value: $GWETTOP})
            CREATE (cloudAmtNode:CLOUD_AMT {name: 'CLOUD_AMT', value: $CLOUD_AMT})
            CREATE (precipNode:PRECTOTCORR {name: 'PRECTOTCORR', value: $PRECTOTCORR})
            
            CREATE (variety:CropVariety {name: 'Rice_Crop_Variety', value: $Rice_Crop_Variety})
            CREATE (seasonNode:Season {name: 'Season', value: $Season})
            CREATE (durationNode:Duration {name: 'Duration', value: $Duration})
            CREATE (daySowing:DayOfSowing {name: 'Day_of_Sowing', value: $Day_of_Sowing})
            
            CREATE (soilPHNode:SoilPH {name: 'soil_pH', value: $soil_pH})
            CREATE (ironNode:Iron {name: 'Iron', value: $Iron})
            CREATE (manganeseNode:Manganese {name: 'Manganese', value: $Manganese})
            CREATE (zincNode:Zinc {name: 'Zinc', value: $Zinc})
            CREATE (copperNode:Copper {name: 'Copper', value: $Copper})
            CREATE (boronNode:Boron {name: 'Boron', value: $Boron})
            CREATE (molyNode:Molybdenum {name: 'Molybdenum', value: $Molybdenum})
            CREATE (chlorineNode:Chlorine {name: 'Chlorine', value: $Chlorine})
            CREATE (nickelNode:Nickel {name: 'Nickel', value: $Nickel})
            CREATE (potassiumNode:Potassium {name: 'Potassium', value: $Potassium})
            CREATE (sodiumNode:Sodium {name: 'Sodium', value: $Sodium})
            CREATE (nitrogenNode:Nitrogen {name: 'Nitrogen', value: $Nitrogen})
            CREATE (calciumNode:Calcium {name: 'Calcium', value: $Calcium})
            CREATE (sulphurNode:Sulphur {name: 'Sulphur', value: $Sulphur})
            CREATE (phosphorousNode:Phosphorous {name: 'Phosphorous', value: $Phosphorous})
            CREATE (yieldTraitNode:Yield_Trait {name: 'Yield_Trait', value: $Yield_Trait})
            
            CREATE (soilMoistureContentNode:Soil_Moisture_Content {name: 'Soil_Moisture_Content', value: $Soil_Moister_Content})
            CREATE (vegetarianIndexNode:Vegetation_Index {name: 'Vegetation_Index', value: $Vegetarian_Index})
            CREATE (humidityNode:RelativeHumidity {name: 'Relative_Humidity', value: $Relative_Humidity})
            
            CREATE (tempNode:Temperature {name: 'Temperature', value: $Temperature})
            CREATE (waterNode:Water {name: 'Water', value: $Water})
            CREATE (radiationNode:Radiation {name: 'Radiation', value: $Radiation})
            
            CREATE (magNode:Magnesium {name: 'Magnesium', value: $Magnesium})
            CREATE (diseaseNode:DiseaseResistance {name: 'Disease_Resistance', value: $Disease_Resistance})
            CREATE (pestNode:PestResistance {name: 'Pest_Resistance', value: $Pest_Resistance})
            CREATE (droughtNode:DroughtTolerance {name: 'Drought_Tolerance', value: $Drought_Tolerance})
            CREATE (heatNode:HeatTolerance {name: 'Heat_Tolerance', value: $Heat_Tolerance})
            CREATE (coldNode:ColdTolerance {name: 'Cold_Tolerance', value: $Cold_Tolerance})
            CREATE (vigorNode:PlantVigor {name: 'Plant_Vigor', value: $Plant_Vigor})
            CREATE (stressNode:PlantStressResilience {name: 'Plant_Stress_Resilience', value: $Plant_Stress_Resilience})
            CREATE (landNode:LandPreparation {name: 'Land_Preparation', value: $Land_Preparation})
            CREATE (bioNode:BiologicalInteractions {name: 'Biological_Interactions', value: $Biological_Interactions})
            CREATE (pollutionNode:Pollution {name: 'Pollution', value: $Pollution})
            CREATE (yieldNode:Yield {name: 'Yield', value: $Yield})
            
            // Build relationships in a chain
            CREATE (rice)-[:HAS_DATE]->(dateNode)
            CREATE (dateNode)-[:HAS_TIME_OF_DAY]->(timeNode)
            CREATE (timeNode)-[:HAS_LAT]->(latNode)
            CREATE (latNode)-[:HAS_LON]->(lonNode)
            CREATE (lonNode)-[:HAS_CROP_STAGE]->(stage)
            CREATE (stage)-[:HAS_TS]->(tsNode)
            CREATE (tsNode)-[:HAS_T2M]->(t2mNode)
            CREATE (t2mNode)-[:HAS_RH2M]->(rh2mNode)
            CREATE (rh2mNode)-[:HAS_WS2M]->(ws2mNode)
            CREATE (ws2mNode)-[:HAS_T2MDEW]->(t2mdewNode)
            CREATE (t2mdewNode)-[:HAS_GWETTOP]->(gwetNode)
            CREATE (gwetNode)-[:HAS_CLOUD_AMT]->(cloudAmtNode)
            CREATE (cloudAmtNode)-[:HAS_PRECTOTCORR]->(precipNode)
            
            CREATE (precipNode)-[:HAS_VARIETY]->(variety)
            CREATE (variety)-[:HAS_SEASON]->(seasonNode)
            CREATE (seasonNode)-[:HAS_DURATION]->(durationNode)
            CREATE (durationNode)-[:HAS_DAY_OF_SOWING]->(daySowing)
            
            CREATE (daySowing)-[:HAS_SOIL_PH]->(soilPHNode)
            CREATE (soilPHNode)-[:HAS_IRON]->(ironNode)
            CREATE (ironNode)-[:HAS_MANGANESE]->(manganeseNode)
            CREATE (manganeseNode)-[:HAS_ZINC]->(zincNode)
            CREATE (zincNode)-[:HAS_COPPER]->(copperNode)
            CREATE (copperNode)-[:HAS_BORON]->(boronNode)
            CREATE (boronNode)-[:HAS_MOLYBDENUM]->(molyNode)
            CREATE (molyNode)-[:HAS_CHLORINE]->(chlorineNode)
            CREATE (chlorineNode)-[:HAS_NICKEL]->(nickelNode)
            CREATE (nickelNode)-[:HAS_POTASSIUM]->(potassiumNode)
            CREATE (potassiumNode)-[:HAS_SODIUM]->(sodiumNode)
            CREATE (sodiumNode)-[:HAS_NITROGEN]->(nitrogenNode)
            CREATE (nitrogenNode)-[:HAS_CALCIUM]->(calciumNode)
            CREATE (calciumNode)-[:HAS_SULPHUR]->(sulphurNode)
            CREATE (sulphurNode)-[:HAS_PHOSPHOROUS]->(phosphorousNode)
            CREATE (phosphorousNode)-[:HAS_YIELD_TRAIT]->(yieldTraitNode)
            CREATE (yieldTraitNode)-[:HAS_SOIL_MOISTURE_CONTENT]->(soilMoistureContentNode)
            CREATE (soilMoistureContentNode)-[:HAS_VEGETATION_INDEX]->(vegetarianIndexNode)
            CREATE (vegetarianIndexNode)-[:HAS_RELATIVE_HUMIDITY]->(humidityNode)
            CREATE (humidityNode)-[:HAS_TEMPERATURE]->(tempNode)
            CREATE (tempNode)-[:HAS_WATER]->(waterNode)
            CREATE (waterNode)-[:HAS_RADIATION]->(radiationNode)
            CREATE (radiationNode)-[:HAS_MAGNESIUM]->(magNode)
            CREATE (magNode)-[:HAS_DISEASE_RESISTANCE]->(diseaseNode)
            CREATE (diseaseNode)-[:HAS_PEST_RESISTANCE]->(pestNode)
            CREATE (pestNode)-[:HAS_DROUGHT_TOLERANCE]->(droughtNode)
            CREATE (droughtNode)-[:HAS_HEAT_TOLERANCE]->(heatNode)
            CREATE (heatNode)-[:HAS_COLD_TOLERANCE]->(coldNode)
            CREATE (coldNode)-[:HAS_PLANT_VIGOR]->(vigorNode)
            CREATE (vigorNode)-[:HAS_PLANT_STRESS_RESILIENCE]->(stressNode)
            CREATE (stressNode)-[:HAS_LAND_PREPARATION]->(landNode)
            CREATE (landNode)-[:HAS_BIOLOGICAL_INTERACTIONS]->(bioNode)
            CREATE (bioNode)-[:HAS_POLLUTION]->(pollutionNode)
            CREATE (pollutionNode)-[:HAS_YIELD]->(yieldNode)
            
            """
            
            parameters = {
                "Date": row["Date"],
                "TimeOfDay": row["TimeOfDay"],
                "LAT": row["LAT"],
                "LON": row["LON"],
                "TS": row["TS"],
                "T2M": row["T2M"],
                "RH2M": row["RH2M"],
                "WS2M": row["WS2M"],
                "T2MDEW": row["T2MDEW"],
                "GWETTOP": row["GWETTOP"],
                "CLOUD_AMT": row["CLOUD_AMT"],
                "PRECTOTCORR": row["PRECTOTCORR"],
                
                "Crop_Stage": row["Crop_Stage"],
                "Rice_Crop_Variety": row["Rice_Crop_Variety"],
                "Season": row["Season"],
                "Duration": row["Duration"],
                "Day_of_Sowing": row["Day_of_Sowing"],
                "soil_pH": row["soil_pH"],
                
                "Iron": row["Iron"],
                "Manganese": row["Manganese"],
                "Zinc": row["Zinc"],
                "Copper": row["Copper"],
                "Boron": row["Boron"],
                "Molybdenum": row["Molybdenum"],
                "Chlorine": row["Chlorine"],
                "Nickel": row["Nickel"],
                "Potassium": row["Potassium"],
                "Sodium": row["Sodium"],
                "Nitrogen": row["Nitrogen"],
                "Calcium": row["Calcium"],
                "Sulphur": row["Sulphur"],
                "Phosphorous": row["Phosphorous"],
                "Yield_Trait": row["Yield_Trait"],
                
                "Soil_Moister_Content": row["Soil_Moister_Content"],
                "Vegetarian_Index": row["Vegetarian_Index"],


                
                "Relative_Humidity": row["Relative_Humidity"],
                "Temperature": row["Temperature"],
                "Water": row["Water"],
                "Radiation": row["Radiation"],
                
                # Additional parameters
                "Magnesium": row["Magnesium"],
                "Disease_Resistance": row["Disease_Resistance"],
                "Pest_Resistance": row["Pest_Resistance"],
                "Drought_Tolerance": row["Drought_Tolerance"],
                "Heat_Tolerance": row["Heat_Tolerance"],
                "Cold_Tolerance": row["Cold_Tolerance"],
                "Plant_Vigor": row["Plant_Vigor"],
                "Plant_Stress_Resilience": row["Plant_Stress_Resilience"],
                "Land_Preparation": row["Land_Preparation"],
                "Biological_Interactions": row["Biological_Interactions"],
                "Pollution": row["Pollution"],
                "Yield": row["Yield"]
            }
            neo4j_connection.execute_query(query, parameters)

        print("Data insertion completed successfully.")

    except Exception as e:
        print(f"Error inserting data into Neo4j: {e}")


def export_data_from_neo4j(connection, output_file):

    try:
        export_query = """
        MATCH (rice:Crop {name: "Rice"})
OPTIONAL MATCH (rice)-[:HAS_DATE]->(dateNode:Date)
OPTIONAL MATCH (dateNode)-[:HAS_TIME_OF_DAY]->(timeNode:TimeOfDay)
OPTIONAL MATCH (timeNode)-[:HAS_LAT]->(latNode:LAT)
OPTIONAL MATCH (latNode)-[:HAS_LON]->(lonNode:LON)
OPTIONAL MATCH (lonNode)-[:HAS_CROP_STAGE]->(stageNode:CropStage)
OPTIONAL MATCH (stageNode)-[:HAS_TS]->(tsNode:TS)
OPTIONAL MATCH (tsNode)-[:HAS_T2M]->(t2mNode:T2M)
OPTIONAL MATCH (t2mNode)-[:HAS_RH2M]->(rh2mNode:RH2M)
OPTIONAL MATCH (rh2mNode)-[:HAS_WS2M]->(ws2mNode:WS2M)
OPTIONAL MATCH (ws2mNode)-[:HAS_T2MDEW]->(t2mdewNode:T2MDEW)
OPTIONAL MATCH (t2mdewNode)-[:HAS_GWETTOP]->(gwetNode:GWETTOP)
OPTIONAL MATCH (gwetNode)-[:HAS_CLOUD_AMT]->(cloudAmtNode:CLOUD_AMT)
OPTIONAL MATCH (cloudAmtNode)-[:HAS_PRECTOTCORR]->(precipNode:PRECTOTCORR)

OPTIONAL MATCH (precipNode)-[:HAS_VARIETY]->(varietyNode:CropVariety)
OPTIONAL MATCH (varietyNode)-[:HAS_SEASON]->(seasonNode:Season)
OPTIONAL MATCH (seasonNode)-[:HAS_DURATION]->(durationNode:Duration)
OPTIONAL MATCH (durationNode)-[:HAS_DAY_OF_SOWING]->(daySowingNode:DayOfSowing)

OPTIONAL MATCH (daySowingNode)-[:HAS_SOIL_PH]->(soilPHNode:SoilPH)
OPTIONAL MATCH (soilPHNode)-[:HAS_IRON]->(ironNode:Iron)
OPTIONAL MATCH (ironNode)-[:HAS_MANGANESE]->(manganeseNode:Manganese)
OPTIONAL MATCH (manganeseNode)-[:HAS_ZINC]->(zincNode:Zinc)
OPTIONAL MATCH (zincNode)-[:HAS_COPPER]->(copperNode:Copper)
OPTIONAL MATCH (copperNode)-[:HAS_BORON]->(boronNode:Boron)
OPTIONAL MATCH (boronNode)-[:HAS_MOLYBDENUM]->(molyNode:Molybdenum)
OPTIONAL MATCH (molyNode)-[:HAS_CHLORINE]->(chlorineNode:Chlorine)
OPTIONAL MATCH (chlorineNode)-[:HAS_NICKEL]->(nickelNode:Nickel)
OPTIONAL MATCH (nickelNode)-[:HAS_POTASSIUM]->(potassiumNode:Potassium)
OPTIONAL MATCH (potassiumNode)-[:HAS_SODIUM]->(sodiumNode:Sodium)
OPTIONAL MATCH (sodiumNode)-[:HAS_NITROGEN]->(nitrogenNode:Nitrogen)
OPTIONAL MATCH (nitrogenNode)-[:HAS_CALCIUM]->(calciumNode:Calcium)
OPTIONAL MATCH (calciumNode)-[:HAS_SULPHUR]->(sulphurNode:Sulphur)
OPTIONAL MATCH (sulphurNode)-[:HAS_PHOSPHOROUS]->(phosphorousNode:Phosphorous)
OPTIONAL MATCH (phosphorousNode)-[:HAS_YIELD_TRAIT]->(yieldTraitNode:Yield_Trait)
OPTIONAL MATCH (yieldTraitNode)-[:HAS_SOIL_MOISTURE_CONTENT]->(soilMoistureContentNode:Soil_Moisture_Content)
OPTIONAL MATCH (soilMoistureContentNode)-[:HAS_VEGETATION_INDEX]->(vegetationIndexNode:Vegetation_Index)
OPTIONAL MATCH (vegetationIndexNode)-[:HAS_RELATIVE_HUMIDITY]->(humidityNode:RelativeHumidity)
OPTIONAL MATCH (humidityNode)-[:HAS_TEMPERATURE]->(tempNode:Temperature)
OPTIONAL MATCH (tempNode)-[:HAS_WATER]->(waterNode:Water)
OPTIONAL MATCH (waterNode)-[:HAS_RADIATION]->(radiationNode:Radiation)
OPTIONAL MATCH (radiationNode)-[:HAS_MAGNESIUM]->(magNode:Magnesium)

OPTIONAL MATCH (magNode)-[:HAS_DISEASE_RESISTANCE]->(diseaseNode:DiseaseResistance)
OPTIONAL MATCH (diseaseNode)-[:HAS_PEST_RESISTANCE]->(pestNode:PestResistance)
OPTIONAL MATCH (pestNode)-[:HAS_DROUGHT_TOLERANCE]->(droughtNode:DroughtTolerance)
OPTIONAL MATCH (droughtNode)-[:HAS_HEAT_TOLERANCE]->(heatNode:HeatTolerance)
OPTIONAL MATCH (heatNode)-[:HAS_COLD_TOLERANCE]->(coldNode:ColdTolerance)

OPTIONAL MATCH (coldNode)-[:HAS_PLANT_VIGOR]->(vigorNode:PlantVigor)
OPTIONAL MATCH (vigorNode)-[:HAS_PLANT_STRESS_RESILIENCE]->(stressNode:PlantStressResilience)
OPTIONAL MATCH (stressNode)-[:HAS_LAND_PREPARATION]->(landNode:LandPreparation)
OPTIONAL MATCH (landNode)-[:HAS_BIOLOGICAL_INTERACTIONS]->(bioNode:BiologicalInteractions)
OPTIONAL MATCH (bioNode)-[:HAS_POLLUTION]->(pollutionNode:Pollution)
OPTIONAL MATCH (pollutionNode)-[:HAS_YIELD]->(yieldNode:Yield)

RETURN
    dateNode.value       AS Date,
    timeNode.value       AS TimeOfDay,
    latNode.value        AS LAT,
    lonNode.value        AS LON,
    stageNode.value      AS Crop_Stage,
    tsNode.value         AS TS,
    t2mNode.value        AS T2M,
    rh2mNode.value       AS RH2M,
    ws2mNode.value       AS WS2M,
    t2mdewNode.value     AS T2MDEW,
    gwetNode.value       AS GWETTOP,
    cloudAmtNode.value   AS CLOUD_AMT,
    precipNode.value     AS PRECTOTCORR,
    varietyNode.value    AS Rice_Crop_Variety,
    seasonNode.value     AS Season,
    durationNode.value   AS Duration,
    daySowingNode.value  AS Day_of_Sowing,
    soilPHNode.value     AS soil_pH,
    ironNode.value       AS Iron,
    manganeseNode.value  AS Manganese,
    zincNode.value       AS Zinc,
    copperNode.value     AS Copper,
    boronNode.value      AS Boron,
    molyNode.value       AS Molybdenum,
    chlorineNode.value   AS Chlorine,
    nickelNode.value     AS Nickel,
    potassiumNode.value  AS Potassium,
    sodiumNode.value     AS Sodium,
    nitrogenNode.value   AS Nitrogen,
    calciumNode.value    AS Calcium,
    sulphurNode.value    AS Sulphur,
    phosphorousNode.value AS Phosphorous,
    yieldTraitNode.value AS Yield_Trait,
    soilMoistureContentNode.value AS Soil_Moisture_Content,
    vegetationIndexNode.value AS Vegetation_Index,
    humidityNode.value   AS Relative_Humidity,
    tempNode.value       AS Temperature,
    waterNode.value      AS Water,
    radiationNode.value  AS Radiation,
    magNode.value        AS Magnesium,
    diseaseNode.value    AS Disease_Resistance,
    pestNode.value       AS Pest_Resistance,
    droughtNode.value    AS Drought_Tolerance,
    heatNode.value       AS Heat_Tolerance,
    coldNode.value       AS Cold_Tolerance,
    vigorNode.value      AS Plant_Vigor,
    stressNode.value     AS Plant_Stress_Resilience,
    landNode.value       AS Land_Preparation,
    bioNode.value        AS Biological_Interactions,
    pollutionNode.value  AS Pollution,
    yieldNode.value      AS Yield


        """
        
        results = connection.execute_query(export_query)
        
        # Convert results (list of dicts) to a Pandas DataFrame
        df_export = pd.DataFrame(results) if results else pd.DataFrame()

        # Write to CSV
        df_export.to_csv(output_file, index=False)
        print(f"Data successfully exported to '{output_file}'")

    except Exception as e:
        print(f"Error exporting data: {e}")

def export_data_from_neo4j(connection, output_file):

    try:
        export_query = """
        MATCH (rice:Crop {name: "Rice"})
OPTIONAL MATCH (rice)-[:HAS_DATE]->(dateNode:Date)
OPTIONAL MATCH (dateNode)-[:HAS_TIME_OF_DAY]->(timeNode:TimeOfDay)
OPTIONAL MATCH (timeNode)-[:HAS_LAT]->(latNode:LAT)
OPTIONAL MATCH (latNode)-[:HAS_LON]->(lonNode:LON)
OPTIONAL MATCH (lonNode)-[:HAS_CROP_STAGE]->(stageNode:CropStage)
OPTIONAL MATCH (stageNode)-[:HAS_TS]->(tsNode:TS)
OPTIONAL MATCH (tsNode)-[:HAS_T2M]->(t2mNode:T2M)
OPTIONAL MATCH (t2mNode)-[:HAS_RH2M]->(rh2mNode:RH2M)
OPTIONAL MATCH (rh2mNode)-[:HAS_WS2M]->(ws2mNode:WS2M)
OPTIONAL MATCH (ws2mNode)-[:HAS_T2MDEW]->(t2mdewNode:T2MDEW)
OPTIONAL MATCH (t2mdewNode)-[:HAS_GWETTOP]->(gwetNode:GWETTOP)
OPTIONAL MATCH (gwetNode)-[:HAS_CLOUD_AMT]->(cloudAmtNode:CLOUD_AMT)
OPTIONAL MATCH (cloudAmtNode)-[:HAS_PRECTOTCORR]->(precipNode:PRECTOTCORR)

OPTIONAL MATCH (precipNode)-[:HAS_VARIETY]->(varietyNode:CropVariety)
OPTIONAL MATCH (varietyNode)-[:HAS_SEASON]->(seasonNode:Season)
OPTIONAL MATCH (seasonNode)-[:HAS_DURATION]->(durationNode:Duration)
OPTIONAL MATCH (durationNode)-[:HAS_DAY_OF_SOWING]->(daySowingNode:DayOfSowing)

OPTIONAL MATCH (daySowingNode)-[:HAS_SOIL_PH]->(soilPHNode:SoilPH)
OPTIONAL MATCH (soilPHNode)-[:HAS_IRON]->(ironNode:Iron)
OPTIONAL MATCH (ironNode)-[:HAS_MANGANESE]->(manganeseNode:Manganese)
OPTIONAL MATCH (manganeseNode)-[:HAS_ZINC]->(zincNode:Zinc)
OPTIONAL MATCH (zincNode)-[:HAS_COPPER]->(copperNode:Copper)
OPTIONAL MATCH (copperNode)-[:HAS_BORON]->(boronNode:Boron)
OPTIONAL MATCH (boronNode)-[:HAS_MOLYBDENUM]->(molyNode:Molybdenum)
OPTIONAL MATCH (molyNode)-[:HAS_CHLORINE]->(chlorineNode:Chlorine)
OPTIONAL MATCH (chlorineNode)-[:HAS_NICKEL]->(nickelNode:Nickel)
OPTIONAL MATCH (nickelNode)-[:HAS_POTASSIUM]->(potassiumNode:Potassium)
OPTIONAL MATCH (potassiumNode)-[:HAS_SODIUM]->(sodiumNode:Sodium)
OPTIONAL MATCH (sodiumNode)-[:HAS_NITROGEN]->(nitrogenNode:Nitrogen)
OPTIONAL MATCH (nitrogenNode)-[:HAS_CALCIUM]->(calciumNode:Calcium)
OPTIONAL MATCH (calciumNode)-[:HAS_SULPHUR]->(sulphurNode:Sulphur)
OPTIONAL MATCH (sulphurNode)-[:HAS_PHOSPHOROUS]->(phosphorousNode:Phosphorous)
OPTIONAL MATCH (phosphorousNode)-[:HAS_YIELD_TRAIT]->(yieldTraitNode:Yield_Trait)
OPTIONAL MATCH (yieldTraitNode)-[:HAS_SOIL_MOISTURE_CONTENT]->(soilMoistureContentNode:Soil_Moisture_Content)
OPTIONAL MATCH (soilMoistureContentNode)-[:HAS_VEGETATION_INDEX]->(vegetationIndexNode:Vegetation_Index)
OPTIONAL MATCH (vegetationIndexNode)-[:HAS_RELATIVE_HUMIDITY]->(humidityNode:RelativeHumidity)
OPTIONAL MATCH (humidityNode)-[:HAS_TEMPERATURE]->(tempNode:Temperature)
OPTIONAL MATCH (tempNode)-[:HAS_WATER]->(waterNode:Water)
OPTIONAL MATCH (waterNode)-[:HAS_RADIATION]->(radiationNode:Radiation)
OPTIONAL MATCH (radiationNode)-[:HAS_MAGNESIUM]->(magNode:Magnesium)

OPTIONAL MATCH (magNode)-[:HAS_DISEASE_RESISTANCE]->(diseaseNode:DiseaseResistance)
OPTIONAL MATCH (diseaseNode)-[:HAS_PEST_RESISTANCE]->(pestNode:PestResistance)
OPTIONAL MATCH (pestNode)-[:HAS_DROUGHT_TOLERANCE]->(droughtNode:DroughtTolerance)
OPTIONAL MATCH (droughtNode)-[:HAS_HEAT_TOLERANCE]->(heatNode:HeatTolerance)
OPTIONAL MATCH (heatNode)-[:HAS_COLD_TOLERANCE]->(coldNode:ColdTolerance)

OPTIONAL MATCH (coldNode)-[:HAS_PLANT_VIGOR]->(vigorNode:PlantVigor)
OPTIONAL MATCH (vigorNode)-[:HAS_PLANT_STRESS_RESILIENCE]->(stressNode:PlantStressResilience)
OPTIONAL MATCH (stressNode)-[:HAS_LAND_PREPARATION]->(landNode:LandPreparation)
OPTIONAL MATCH (landNode)-[:HAS_BIOLOGICAL_INTERACTIONS]->(bioNode:BiologicalInteractions)
OPTIONAL MATCH (bioNode)-[:HAS_POLLUTION]->(pollutionNode:Pollution)
OPTIONAL MATCH (pollutionNode)-[:HAS_YIELD]->(yieldNode:Yield)

RETURN
    dateNode.value       AS Date,
    timeNode.value       AS TimeOfDay,
    latNode.value        AS LAT,
    lonNode.value        AS LON,
    stageNode.value      AS Crop_Stage,
    tsNode.value         AS TS,
    t2mNode.value        AS T2M,
    rh2mNode.value       AS RH2M,
    ws2mNode.value       AS WS2M,
    t2mdewNode.value     AS T2MDEW,
    gwetNode.value       AS GWETTOP,
    cloudAmtNode.value   AS CLOUD_AMT,
    precipNode.value     AS PRECTOTCORR,
    varietyNode.value    AS Rice_Crop_Variety,
    seasonNode.value     AS Season,
    durationNode.value   AS Duration,
    daySowingNode.value  AS Day_of_Sowing,
    soilPHNode.value     AS soil_pH,
    ironNode.value       AS Iron,
    manganeseNode.value  AS Manganese,
    zincNode.value       AS Zinc,
    copperNode.value     AS Copper,
    boronNode.value      AS Boron,
    molyNode.value       AS Molybdenum,
    chlorineNode.value   AS Chlorine,
    nickelNode.value     AS Nickel,
    potassiumNode.value  AS Potassium,
    sodiumNode.value     AS Sodium,
    nitrogenNode.value   AS Nitrogen,
    calciumNode.value    AS Calcium,
    sulphurNode.value    AS Sulphur,
    phosphorousNode.value AS Phosphorous,
    yieldTraitNode.value AS Yield_Trait,
    soilMoistureContentNode.value AS Soil_Moisture_Content,
    vegetationIndexNode.value AS Vegetation_Index,
    humidityNode.value   AS Relative_Humidity,
    tempNode.value       AS Temperature,
    waterNode.value      AS Water,
    radiationNode.value  AS Radiation,
    magNode.value        AS Magnesium,
    diseaseNode.value    AS Disease_Resistance,
    pestNode.value       AS Pest_Resistance,
    droughtNode.value    AS Drought_Tolerance,
    heatNode.value       AS Heat_Tolerance,
    coldNode.value       AS Cold_Tolerance,
    vigorNode.value      AS Plant_Vigor,
    stressNode.value     AS Plant_Stress_Resilience,
    landNode.value       AS Land_Preparation,
    bioNode.value        AS Biological_Interactions,
    pollutionNode.value  AS Pollution,
    yieldNode.value      AS Yield


        """
        
        results = connection.execute_query(export_query)
        
        # Convert results (list of dicts) to a Pandas DataFrame
        df_export = pd.DataFrame(results) if results else pd.DataFrame()

        # Write to CSV
        df_export.to_csv(output_file, index=False)
        print(f"Data successfully exported to '{output_file}'")

    except Exception as e:
        print(f"Error exporting data: {e}")

if __name__ == "__main__":
    
    uri = "bolt://192.168.0.36:7687"
    user = "neo4j"
    password = "Naveen1@"
    connection = Neo4jConnection(uri, user, password)

    insert_data_into_neo4j(merged_df, connection)

    output_file = "neo4j_sowing_export.csv"
    export_data_from_neo4j(connection, output_file)

    connection.close()




