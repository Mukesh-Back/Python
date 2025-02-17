import random
import pandas as pd
from datetime import datetime, timedelta
from neo4j import GraphDatabase

class Neo4jConnection:
    def __init__(self, uri, user, password, db_name="Germination"):
        try:
            self._driver = GraphDatabase.driver(uri, auth=(user, password))
            self.db_name = db_name 
        except Exception as e:
            raise ConnectionError(f"Error connecting to Neo4j: {e}")

    def close(self):
        if self._driver:
            self._driver.close()

    def execute_query(self, query, parameters=None):
        try:
            with self._driver.session(database=self.db_name) as session:
                return session.run(query, parameters).data()
        except Exception as e:
            print(f"Error executing query: {e}")
            return []

def calculate_germination_potential(row, germination_parameters):
    """
    Calculate germination potential based on weighted parameters.
    """
    try:
        germination_score = 0

        germination_score += (row["Soil_Moisture_Content"] - 70) / (90 - 70) * 20
        germination_score += (row["Soil_Temperature"] - 20) / (30 - 20) * 20
        germination_score += (row["Seed_Vigor_Index"] - 70) / (100 - 70) * 15
        germination_score += (row["Sunlight_Exposure"] - 6) / (12 - 6) * 10
        germination_score += (row["Disease_Resistance"] / 100) * 15

        return max(0, min(100, round(germination_score, 2)))
    except Exception as e:
        print(f"Error calculating germination potential: {e}")
        return 0

def generate_germination_stage_dataset():
    try:
        # Input date range
        start_date_input = input("Enter the start date (YYYY-MM-DD): ")
        end_date_input = input("Enter the end date (YYYY-MM-DD): ")

        start_date = datetime.strptime(start_date_input, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_input, '%Y-%m-%d')

        if start_date > end_date:
            raise ValueError("Start date must be earlier than end date.")

        # Define germination parameters and crop varieties
        germination_parameters = {
            "PH":(5.5, 7),
            "Soil_Moisture_Content": (70, 90),
            "Soil_Temperature": (20, 30),
            "Soil_pH": (5.5, 6.5),
            "Daily_Watering_Level": (10, 20),
            "Water_Retention_Capacity": (50, 90),
            "Ambient_Temperature": (20, 30),
            "Temperature": (20, 35),
            "Sunlight_Exposure": (6, 12),
            "Rainfall": (50, 300),
            "Nitrogen": (15, 20),
            "Phosphorus": (2, 3),
            "Zinc": (0.5, 2),
            "Iron": (4, 10),
            "Manganese":(10, 50),
            "Copper":(0.2, 0.5),
            "Boron":(0.3, 1),
            "Molybdenum":(0.01, 0.05),
            "Chlorine":(0.2, 2.0),
            "Nickel":(0.01, 0.1),
            "Calcium":(15, 30),
            "Sulphur":(15, 30),
            "Relative_Humidity":(60, 85),
            "Water":(2.5, 5),
            "Radiation":(300, 500),
            "Magnesium": (10, 50),
            "Sodium": (15, 20),
            "Radiation_Exposure":(1,10),#
            "Seed_Vigor_Index": (70, 100),
            "Germination_Rate": (80, 100),
            "Disease_Resistance": (0, 100),
            "Pest_Resistance": (0, 100),
            "Planting_Depth": (2, 5),
            "Seed_Spacing": (5, 10),
            "Vegetation_Index": (0.3, 0.9),
            "Yield": (0, 100),
            "Seed_Viability": (70, 100),
            "Seed_Moisture_Content": (8, 12),
            "Heat_Stress_Index": (0, 1),
            "Water_Stress_Index": (0, 1),
            "Salinity_Stress_Index": (0, 1),
            "Root_Growth_Rate": (1, 5),
            "Shoot_Emergence_Time": (3, 10),
            "Soil_Respiration_Rate": (0, 10),
            "Organic_Matter_Content": (1, 6),
            "Cation_Exchange_Capacity": (10, 40),
            "Electrical_Conductivity": (0.2, 1.2),
            "Wind_Speed": (0, 10),
            "Cloud_Cover": (0, 100),
            "Initial_Fertilizer_Application": ["Urea", "DAP", "Potash"]
        }

        crop_varieties = [
            {"type": "Short", "duration_range": (90, 110)},
            {"type": "Medium", "duration_range": (110, 140)},
            {"type": "Long", "duration_range": (140, 160)},
        ]

        # Data container
        data = []
        current_date = start_date
        total_days = (end_date - start_date).days + 1
        times_of_day = ['morning', 'afternoon', 'evening']

        # Initialize day of crop (8–20 day cycling)
        day_of_crop = 8

        # Generate data
        for day in range(total_days):
            # Determine the season based on the month
            season = "Rabi" if current_date.month in [11, 12, 1, 2, 3] else "Kharif"

            # Randomly select a crop variety
            crop_variety = random.choice(crop_varieties)
            duration = random.randint(*crop_variety["duration_range"])

            for time_of_day in times_of_day:
                row = {
                    'Date': current_date.strftime('%d/%m/%Y'),
                    'TimeOfDay': time_of_day,
                    "Crop_Stage": "Germination",
                    "Crop_Variety_Type": crop_variety["type"],
                    "Duration": duration,
                    "Season": season,
                    "Day_of_Crop": day_of_crop
                }

                # Populate germination parameters
                for param, value_range in germination_parameters.items():
                    if isinstance(value_range, tuple):
                        row[param] = round(random.uniform(*value_range), 2) if isinstance(value_range[0], float) else random.randint(*value_range)
                    else:
                        row[param] = random.choice(value_range)

                # Calculate germination potential and yield
                row["Germination_Potential"] = calculate_germination_potential(row, germination_parameters)
                row["Yield"] = round(random.uniform(0, 100), 2)

                data.append(row)

            # Increment date
            current_date += timedelta(days=1)

            # Cycle day_of_crop between 8 and 20
            day_of_crop += 1
            if day_of_crop > 20:
                day_of_crop = 8

        return pd.DataFrame(data)
    except Exception as e:
        print(f"Error generating dataset: {e}")
        return pd.DataFrame()

# Example Usage


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


# Generate and save the dataset
dataset = generate_germination_stage_dataset()
power_df = pd.read_csv("power.csv")
# Merge Power data with Sowing data
merged_df = merge_power_data(dataset, power_df)
# Save final CSV
merged_df.to_csv("Germination_stage_dataset_last.csv", index=False)
print("Dataset generated and saved to 'Germination_stage_dataset.csv'.")
desired_columns = [
    "Date", "TimeOfDay", "LAT", "LON", "TS", "T2M", "RH2M", "WS2M", "T2MDEW", "GWETTOP",
    "CLOUD_AMT", "PRECTOTCORR", "Crop_Stage", "Crop_Variety_Type", "Season", "Duration",
    "Day_of_Crop", "Soil_Moisture_Content", "Soil_Temperature", "Soil_pH", "Daily_Watering_Level",
    "Water_Retention_Capacity", "Ambient_Temperature", "Humidity", "Sunlight_Exposure",
    "Rainfall", "Nitrogen", "Phosphorus", "Potassium", "Zinc", "Iron", "Magnesium", "Sodium",
    "Seed_Vigor_Index", "Germination_Rate", "Disease_Resistance", "Pest_Resistance",
    "Planting_Depth", "Seed_Spacing", "Vegetation_Index", "Seed_Viability",
    "Seed_Moisture_Content", "Heat_Stress_Index", "Water_Stress_Index", "Salinity_Stress_Index",
    "Root_Growth_Rate", "Shoot_Emergence_Time", "Soil_Respiration_Rate", "Organic_Matter_Content",
    "Cation_Exchange_Capacity", "Electrical_Conductivity", "Wind_Speed", "Cloud_Cover",
    "Radiation_Exposure", "Initial_Fertilizer_Application","Yield","Boron","Chlorine","Copper","Manganese",
    "Molybdenum","Nickel","Calcium","Sulphur","Phosphorous","Radiation","Water"
]

# Reindex the DataFrame to match desired columns
merged_df = merged_df.reindex(columns=desired_columns)
# merged_df.to_csv("Germination_stage_dataset3.csv", index=False)

def create_germination_relationships_in_neo4j(merged_df, neo4j_connection):
    try:
        # Create the "Rice" crop node
        neo4j_connection.execute_query("MERGE (rice:Crop {name: 'Rice'})")
        
        # Iterate over the DataFrame row by row
        for _, row in merged_df.iterrows():
            query = """
            MATCH (crop:Crop {name: 'Rice'})    
            CREATE (date:Date {name: "Date", value: $Date})
            CREATE (timeOfDay:TimeOfDay {name: "TimeOfDay", value: $TimeOfDay})
            CREATE (lat:LAT {name: "LAT", value: $LAT})
            CREATE (lon:LON {name: "LON", value: $LON})
            CREATE (ts:TS {name: "TS", value: $TS})
            CREATE (t2m:T2M {name: "T2M", value: $T2M})
            CREATE (rh2m:RH2M {name: "RH2M", value: $RH2M})
            CREATE (ws2m:WS2M {name: "WS2M", value: $WS2M})
            CREATE (t2mdew:T2MDEW {name: "T2MDEW", value: $T2MDEW})
            CREATE (gwettop:GWETTOP {name: "GWETTOP", value: $GWETTOP})
            CREATE (cloudAmt:CLOUD_AMT {name: "CLOUD_AMT", value: $CLOUD_AMT})
            CREATE (prectotcorr:PRECTOTCORR {name: "PRECTOTCORR", value: $PRECTOTCORR})
            CREATE (cropStage:CropStage {name: "CropStage", value: $Crop_Stage})
            CREATE (variety:CropVarietyType {name: "CropVarietyType", value: $Crop_Variety_Type})
            CREATE (season:Season {name: "Season", value: $Season})
            CREATE (duration:Duration {name: "Duration", value: $Duration})
            CREATE (dayOfCrop:DayOfCrop {name: "DayOfCrop", value: $Day_of_Crop})
            CREATE (soilMoistureContent:SoilMoistureContent {name: "SoilMoistureContent", value: $Soil_Moisture_Content})
            CREATE (soilTemperature:SoilTemperature {name: "SoilTemperature", value: $Soil_Temperature})
            CREATE (soilPH:SoilPH {name: "SoilPH", value: $Soil_pH})
            CREATE (dailyWateringLevel:DailyWateringLevel {name: "DailyWateringLevel", value: $Daily_Watering_Level})
            CREATE (waterRetentionCapacity:WaterRetentionCapacity {name: "WaterRetentionCapacity", value: $Water_Retention_Capacity})
            CREATE (ambientTemperature:AmbientTemperature {name: "AmbientTemperature", value: $Ambient_Temperature})
            CREATE (humidity:Humidity {name: "Humidity", value: $Humidity})
            CREATE (sunlightExposure:SunlightExposure {name: "SunlightExposure", value: $Sunlight_Exposure})
            CREATE (rainfall:Rainfall {name: "Rainfall", value: $Rainfall})
            CREATE (nitrogen:Nitrogen {name: "Nitrogen", value: $Nitrogen})
            CREATE (phosphorus:Phosphorus {name: "Phosphorus", value: $Phosphorus})
            CREATE (potassium:Potassium {name: "Potassium", value: $Potassium})
            CREATE (zinc:Zinc {name: "Zinc", value: $Zinc})
            CREATE (iron:Iron {name: "Iron", value: $Iron})
            CREATE (magnesium:Magnesium {name: "Magnesium", value: $Magnesium})

            CREATE (boron:Boron {name: "Boron", value: $Boron})
            CREATE (chlorine:Chlorine {name: "Chlorine", value: $Chlorine})
            CREATE (copper:Copper {name: "Copper", value: $Copper})
            CREATE (manganese:Manganese {name: "Manganese", value: $Manganese})
            CREATE (molybdenum:Molybdenum {name: "Molybdenum", value: $Molybdenum})
            CREATE (nickel:Nickel {name: "Nickel", value: $Nickel})
            CREATE (calcium:Calcium {name: "Calcium", value: $Calcium})
            CREATE (sulphur:Sulphur {name: "Sulphur", value: $Sulphur})
            CREATE (phosphorous:Phosphorous {name: "Phosphorous", value: $Phosphorous})
            CREATE (radiation:Radiation {name: "Radiation", value: $Radiation})
            CREATE (water:Water {name: "Water", value: $Water})
            

            CREATE (sodium:Sodium {name: "Sodium", value: $Sodium})
            CREATE (seedVigorIndex:SeedVigorIndex {name: "SeedVigorIndex", value: $SeedVigorIndex})
            CREATE (germinationRate:GerminationRate {name: "GerminationRate", value: $GerminationRate})
            CREATE (diseaseResistance:DiseaseResistance {name: "DiseaseResistance", value: $DiseaseResistance})
            CREATE (pestResistance:PestResistance {name: "PestResistance", value: $PestResistance})
            CREATE (plantingDepth:PlantingDepth {name: "PlantingDepth", value: $PlantingDepth})
            CREATE (seedSpacing:SeedSpacing {name: "SeedSpacing", value: $SeedSpacing})
            CREATE (vegetationIndex:VegetationIndex {name: "VegetationIndex", value: $VegetationIndex})
            CREATE (seedViability:SeedViability {name: "SeedViability", value: $SeedViability})
            CREATE (seedMoistureContent:SeedMoistureContent {name: "SeedMoistureContent", value: $SeedMoistureContent})
            CREATE (heatStressIndex:HeatStressIndex {name: "HeatStressIndex", value: $HeatStressIndex})
            CREATE (waterStressIndex:WaterStressIndex {name: "WaterStressIndex", value: $WaterStressIndex})
            CREATE (salinityStressIndex:SalinityStressIndex {name: "SalinityStressIndex", value: $SalinityStressIndex})
            CREATE (rootGrowthRate:RootGrowthRate {name: "RootGrowthRate", value: $RootGrowthRate})
            CREATE (shootEmergenceTime:ShootEmergenceTime {name: "ShootEmergenceTime", value: $ShootEmergenceTime})
            CREATE (soilRespirationRate:SoilRespirationRate {name: "SoilRespirationRate", value: $SoilRespirationRate})
            CREATE (organicMatterContent:OrganicMatterContent {name: "OrganicMatterContent", value: $OrganicMatterContent})
            CREATE (cationExchangeCapacity:CationExchangeCapacity {name: "CationExchangeCapacity", value: $CationExchangeCapacity})
            CREATE (electricalConductivity:ElectricalConductivity {name: "ElectricalConductivity", value: $ElectricalConductivity})
            CREATE (windSpeed:WindSpeed {name: "WindSpeed", value: $WindSpeed})
            CREATE (cloudCover:CloudCover {name: "CloudCover", value: $CloudCover})
            CREATE (radiationExposure:RadiationExposure {name: "RadiationExposure", value: $RadiationExposure})
            CREATE (initialFertilizerApplication:InitialFertilizerApplication {name: "InitialFertilizerApplication", value: $InitialFertilizerApplication})
            CREATE (yieldNode:Yield {name: "Yield", value: $Yield})

            // Relationships
            CREATE (date)-[:HAS_TIME_OF_DAY]->(timeOfDay)
            CREATE (crop)-[:HAS_DATE]->(date)
            CREATE (timeOfDay)-[:HAS_LAT]->(lat)
            CREATE (lat)-[:HAS_LON]->(lon)
            CREATE (lon)-[:HAS_TS]->(ts)
            CREATE (ts)-[:HAS_T2M]->(t2m)
            CREATE (t2m)-[:HAS_RH2M]->(rh2m)
            CREATE (rh2m)-[:HAS_WS2M]->(ws2m)
            CREATE (ws2m)-[:HAS_T2MDEW]->(t2mdew)
            CREATE (t2mdew)-[:HAS_GWETTOP]->(gwettop)
            CREATE (gwettop)-[:HAS_CLOUD_AMT]->(cloudAmt)
            CREATE (cloudAmt)-[:HAS_PRECTOTCORR]->(prectotcorr)
            CREATE (prectotcorr)-[:HAS_CROP_STAGE]->(cropStage)
            CREATE (cropStage)-[:HAS_VARIETY]->(variety)
            CREATE (variety)-[:HAS_SEASON]->(season)
            CREATE (season)-[:HAS_DURATION]->(duration)
            CREATE (duration)-[:HAS_DAY_OF_CROP]->(dayOfCrop)
            CREATE (dayOfCrop)-[:HAS_SOIL_MOISTURE_CONTENT]->(soilMoistureContent)
            CREATE (soilMoistureContent)-[:HAS_SOIL_TEMPERATURE]->(soilTemperature)
            CREATE (soilTemperature)-[:HAS_SOIL_PH]->(soilPH)
            CREATE (soilPH)-[:HAS_DAILY_WATERING_LEVEL]->(dailyWateringLevel)
            CREATE (dailyWateringLevel)-[:HAS_WATER_RETENTION_CAPACITY]->(waterRetentionCapacity)
            CREATE (waterRetentionCapacity)-[:HAS_AMBIENT_TEMPERATURE]->(ambientTemperature)
            CREATE (ambientTemperature)-[:HAS_HUMIDITY]->(humidity)
            CREATE (humidity)-[:HAS_SUNLIGHT_EXPOSURE]->(sunlightExposure)
            CREATE (sunlightExposure)-[:HAS_RAINFALL]->(rainfall)
            CREATE (rainfall)-[:HAS_NITROGEN]->(nitrogen)
            CREATE (nitrogen)-[:HAS_PHOSPHORUS]->(phosphorus)
            CREATE (phosphorus)-[:HAS_POTASSIUM]->(potassium)
            CREATE (potassium)-[:HAS_ZINC]->(zinc)
            CREATE (zinc)-[:HAS_IRON]->(iron)
            CREATE (iron)-[:HAS_MAGNESIUM]->(magnesium)
            CREATE (magnesium)-[:HAS_BORON]->(boron)

            CREATE (boron)-[:HAS_CHLORINE]->(chlorine)
            CREATE (chlorine)-[:HAS_COPPER]->(copper)
            CREATE (copper)-[:HAS_MANGANESE]->(manganese)
            CREATE (manganese)-[:HAS_MOLYBDENUM]->(molybdenum)
            CREATE (molybdenum)-[:HAS_NICKEL]->(nickel)
            CREATE (nickel)-[:HAS_CALCIUM]->(calcium)
            CREATE (calcium)-[:HAS_SULPHUR]->(sulphur)
            CREATE (sulphur)-[:HAS_PHOSPHOROUS]->(phosphorous)
            CREATE (phosphorous)-[:HAS_RADIATION]->(radiation)
            CREATE (radiation)-[:HAS_WATER]->(water)
            CREATE (water)-[:HAS_SODIUM]->(sodium)





            CREATE (sodium)-[:HAS_SEED_VIGOR_INDEX]->(seedVigorIndex)
            CREATE (seedVigorIndex)-[:HAS_GERMINATION_RATE]->(germinationRate)
            CREATE (germinationRate)-[:HAS_DISEASE_RESISTANCE]->(diseaseResistance)
            CREATE (diseaseResistance)-[:HAS_PEST_RESISTANCE]->(pestResistance)
            CREATE (pestResistance)-[:HAS_PLANTING_DEPTH]->(plantingDepth)
            CREATE (plantingDepth)-[:HAS_SEED_SPACING]->(seedSpacing)
            CREATE (seedSpacing)-[:HAS_VEGETATION_INDEX]->(vegetationIndex)
            CREATE (vegetationIndex)-[:HAS_SEED_VIABILITY]->(seedViability)
            CREATE (seedViability)-[:HAS_SEED_MOISTURE_CONTENT]->(seedMoistureContent)
            CREATE (seedMoistureContent)-[:HAS_HEAT_STRESS_INDEX]->(heatStressIndex)
            CREATE (heatStressIndex)-[:HAS_WATER_STRESS_INDEX]->(waterStressIndex)
            CREATE (waterStressIndex)-[:HAS_SALINITY_STRESS_INDEX]->(salinityStressIndex)
            CREATE (salinityStressIndex)-[:HAS_ROOT_GROWTH_RATE]->(rootGrowthRate)
            CREATE (rootGrowthRate)-[:HAS_SHOOT_EMERGENCE_TIME]->(shootEmergenceTime)
            CREATE (shootEmergenceTime)-[:HAS_SOIL_RESPIRATION_RATE]->(soilRespirationRate)
            CREATE (soilRespirationRate)-[:HAS_ORGANIC_MATTER_CONTENT]->(organicMatterContent)
            CREATE (organicMatterContent)-[:HAS_CATION_EXCHANGE_CAPACITY]->(cationExchangeCapacity)
            CREATE (cationExchangeCapacity)-[:HAS_ELECTRICAL_CONDUCTIVITY]->(electricalConductivity)
            CREATE (electricalConductivity)-[:HAS_WIND_SPEED]->(windSpeed)
            CREATE (windSpeed)-[:HAS_CLOUD_COVER]->(cloudCover)
            CREATE (cloudCover)-[:HAS_RADIATION_EXPOSURE]->(radiationExposure)
            CREATE (radiationExposure)-[:HAS_INITIAL_FERTILIZER_APPLICATION]->(initialFertilizerApplication)
            CREATE (initialFertilizerApplication)-[:HAS_YIELD]->(yieldNode)

            """
            
            # Define parameters from the row
            params = {
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
                "Crop_Variety_Type": row["Crop_Variety_Type"],
                "Season": row["Season"],
                "Duration": row["Duration"],
                "Day_of_Crop": row["Day_of_Crop"],
                "Soil_Moisture_Content": row["Soil_Moisture_Content"],
                "Soil_Temperature": row["Soil_Temperature"],
                "Soil_pH": row["Soil_pH"],
                "Daily_Watering_Level": row["Daily_Watering_Level"],
                "Water_Retention_Capacity": row["Water_Retention_Capacity"],
                "Ambient_Temperature": row["Ambient_Temperature"],
                "Humidity": row["Humidity"],
                "Sunlight_Exposure": row["Sunlight_Exposure"],
                "Rainfall": row["Rainfall"],
                "Nitrogen": row["Nitrogen"],
                "Phosphorus": row["Phosphorus"],
                "Potassium": row["Potassium"],
                "Zinc": row["Zinc"],
                "Iron": row["Iron"],
                "Magnesium": row["Magnesium"],


                "Boron": row["Boron"],
                "Chlorine": row["Chlorine"],
                "Copper": row["Copper"],
                "Manganese": row["Manganese"],
                "Molybdenum": row["Molybdenum"],
                "Nickel": row["Nickel"],
                "Calcium": row["Calcium"],
                "Sulphur": row["Sulphur"],
                "Phosphorous": row["Phosphorous"],
                "Radiation": row["Radiation"],
                "Water": row["Water"],


                "Sodium": row["Sodium"],
                "SeedVigorIndex": row["Seed_Vigor_Index"],
                "GerminationRate": row["Germination_Rate"],
                "DiseaseResistance": row["Disease_Resistance"],
                "PestResistance": row["Pest_Resistance"],
                "PlantingDepth": row["Planting_Depth"],
                "SeedSpacing": row["Seed_Spacing"],
                "VegetationIndex": row["Vegetation_Index"],
                "SeedViability": row["Seed_Viability"],
                "SeedMoistureContent": row["Seed_Moisture_Content"],
                "HeatStressIndex": row["Heat_Stress_Index"],
                "WaterStressIndex": row["Water_Stress_Index"],
                "SalinityStressIndex": row["Salinity_Stress_Index"],
                "RootGrowthRate": row["Root_Growth_Rate"],
                "ShootEmergenceTime": row["Shoot_Emergence_Time"],
                "SoilRespirationRate": row["Soil_Respiration_Rate"],
                "OrganicMatterContent": row["Organic_Matter_Content"],
                "CationExchangeCapacity": row["Cation_Exchange_Capacity"],
                "ElectricalConductivity": row["Electrical_Conductivity"],
                "WindSpeed": row["Wind_Speed"],
                "CloudCover": row["Cloud_Cover"],
                "RadiationExposure": row["Radiation_Exposure"],
                "InitialFertilizerApplication": row["Initial_Fertilizer_Application"],
                "Yield": row["Yield"]
            }

            # Execute the query
            neo4j_connection.execute_query(query, params)
        
        print("Nodes and relationships successfully created in Neo4j!")
    except Exception as e:
        print(f"Error creating nodes and relationships in Neo4j: {e}")

# Connection details
uri = "bolt://localhost:7687"
user = "neo4j"  
password = "Naveen1@"
db_name="Germination"
neo4j_conn = Neo4jConnection(uri, user, password,db_name="Germination")
create_germination_relationships_in_neo4j(merged_df,neo4j_conn)

neo4j_conn.close()


"""
#add :
        # radiation
        # potassium 





#check
    # germination :
            # Select Date
            # Time Of Day
            # Crop Stage
            # Days 
            # Rice Crop Variety
            # Duration
            # Season
            # Latitude
            # Longitude
            # Soil PH
            # Sodium
            # Nitrogen 
            # Magnesium 
            # Boron
            # Chlorine 
            # Copper
            # Iron 
            # Manganese
            # Molybdenum
            # Zinc
            # Nickel
            # Calcium
            # Sulphur 
            # Phosphorous
            # Potassium 
            # Cation Exchange Capacity
            # Electrical Conductivity
            # Skin Temperature
            # Temperature at 2 Meters
            # Relative Humidity at 2 Meters
            # Wind Speed at 2 Meters
            # Dew/Frost Point at 2 Meters
            # Surface Soil Wetness
            # Cloud Amount
            # Precipitation Corrected
            # Water Requirement
            # Radiation
            # Soil Moisture Content
            # Soil Temperature
            # Water Retention Capacity
            # Ambient Temperature
            # Humidity 
            # Sunlight Exposure
            # Rainfall
            # Wind Speed
            # Cloud Cover 
            # Radiation Exposure
            # Seed Vigor Index
            # Germination Rate 
            # Disease Resistance
            # Pest Resistance
            # Root Growth Rate 
            # Shoot Emergence Time
            # Planting Depth
            # Seed Spacing
            # Vegetation Index 
            # Seed Viability
            # Seed Moisture Content
            # Initial Fertilizer Application
            # Heat Stress Index
            # Water Stress Index
            # Salinity Stress Index 
            # Soil Respiration Rate
            # Organic Matter Content
"""
