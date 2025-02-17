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
            "Soil_pH": (5.5, 7.0),
            "Sodium": (15, 20),  # mg/kg
            "Nitrogen": (15, 20),  # mg/kg
            "Magnesium": (10, 50),  # mg/kg
            "Boron": (0.3, 1.0),  # mg/kg
            "Chlorine": (0.2, 2.0),  # mg/kg
            "Copper": (0.2, 0.5),  # mg/kg
            "Iron": (4, 10),  # mg/kg
            "Manganese": (10, 50),  # mg/kg
            "Molybdenum": (0.01, 0.05),  # mg/kg
            "Zinc": (0.5, 2.0),  # mg/kg
            "Nickel": (0.01, 0.1),  # mg/kg
            "Calcium": (15, 30),  # mg/kg
            "Sulphur": (15, 30),  # mg/kg
            "Phosphorous": (2, 3),  # mg/kg
            "Potassium": (15, 20),  # mg/kg
            "Cation_Exchange_Capacity": (10, 40),  # meq/100g
            "Electrical_Conductivity": (0.2, 1.2),  # dS/m
            "Water_Requirement": (2.5, 5),  # liters/day
            "Radiation": (300, 500),  # W/m²
            "Soil_Moisture_Content": (70, 90),  # %
            "Soil_Temperature": (20, 35),  # °C
            "Water_Retention_Capacity": (50, 90),  # %
            "Ambient_Temperature": (20, 35),  # °C
            "Humidity": (60, 85),  # %
            "Sunlight_Exposure": (6, 12),  # hours
            "Rainfall": (50, 300),  # mm
            "Wind_Speed": (0, 10),  # m/s
            "Cloud_Cover": (0, 100),  # %
            "Radiation_Exposure": (300, 500),  # W/m²
            "Seed_Vigor_Index": (70, 100),  # %
            "Germination_Rate": (80, 100),  # %
            "Disease_Resistance": (0, 100),  # %
            "Pest_Resistance": (0, 100),  # %
            "Root_Growth_Rate": (1, 5),  # cm/day
            "Shoot_Emergence_Time": (3, 10),  # days
            "Planting_Depth": (2, 5),  # cm
            "Seed_Spacing": (5, 10),  # cm
            "Vegetation_Index": (0.3, 0.9),  # NDVI
            "Seed_Viability": (70, 100),  # %
            "Seed_Moisture_Content": (8, 12),  # %
            "Initial_Fertilizer_Application": ["Urea", "DAP", "Potash"],
            "Heat_Stress_Index": (0, 1),  # Index
            "Water_Stress_Index": (0, 1),  # Index
            "Salinity_Stress_Index": (0, 1),  # Index
            "Soil_Respiration_Rate": (0, 10),  # mg CO₂/m²/h
            "Organic_Matter_Content": (1, 6)  # %  # Ensure Yield is only defined here
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
                        if isinstance(value_range[0], float) or isinstance(value_range[1], float):
                            row[param] = round(random.uniform(*value_range), 2)
                        else:
                            row[param] = random.randint(*value_range)
                    else:
                        row[param] = random.choice(value_range)

                # Yield is already assigned via germination_parameters
                row["Yield"] = random.randint(0, 100)

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
# merged_df.to_csv("Germination_stage_dataset_last.csv", index=False)
# print("Dataset generated and saved to 'Germination_stage_dataset.csv'.")
desired_columns = [
            "Date", "TimeOfDay", "LAT", "LON", "TS", "T2M", "RH2M", "WS2M",
            "T2MDEW", "GWETTOP", "CLOUD_AMT", "PRECTOTCORR",
            "Crop_Stage", "Crop_Variety_Type", "Season", "Duration",
            "Day_of_Crop", "Soil_Moisture_Content","Soil_Temperature", "Soil_pH", "Sodium",
            "Nitrogen", "Magnesium", "Boron", "Chlorine", "Copper",
            "Iron", "Manganese", "Molybdenum", "Zinc", "Nickel","Water_Retention_Capacity",
            "Calcium", "Sulphur", "Phosphorous", "Potassium",
            "Cation_Exchange_Capacity", "Electrical_Conductivity",
            "Water_Requirement", "Radiation", "Ambient_Temperature",
            "Humidity", "Sunlight_Exposure", "Rainfall", "Wind_Speed",
            "Cloud_Cover", "Radiation_Exposure", "Seed_Vigor_Index",
            "Germination_Rate", "Disease_Resistance", "Pest_Resistance",
            "Root_Growth_Rate", "Shoot_Emergence_Time", "Planting_Depth",
            "Seed_Spacing", "Vegetation_Index", "Seed_Viability",
            "Seed_Moisture_Content", "Initial_Fertilizer_Application",
            "Heat_Stress_Index", "Water_Stress_Index", "Salinity_Stress_Index",
            "Soil_Respiration_Rate", "Organic_Matter_Content","Yield"
        ]


# Reindex the DataFrame to match desired columns
merged_df = merged_df.reindex(columns=desired_columns)
# merged_df.to_csv("Germination_stage_dataset333333.csv", index=False)

def create_germination_relationships_in_neo4j(merged_df, neo4j_connection):
    try:
        # Create the "Rice" crop node
        crop_query = """
        MERGE (crop:Crop {name: 'Rice'})
        """
        neo4j_connection.execute_query(crop_query)
        print("Crop node created.")

        # Iterate over the DataFrame row by row
        for _, row in merged_df.iterrows():
            query = """
            MATCH (crop:Crop {name: 'Rice'})    
            CREATE (dateNode:Date {name: "Date", value: $Date})
            CREATE (timeNode:TimeOfDay {name: "TimeOfDay", value: $TimeOfDay})
            CREATE (latNode:LAT {name: "LAT", value: $LAT})
            CREATE (lonNode:LON {name: "LON", value: $LON})
            CREATE (tsNode:TS {name: "TS", value: $TS})
            CREATE (t2mNode:T2M {name: "T2M", value: $T2M})
            CREATE (rh2mNode:RH2M {name: "RH2M", value: $RH2M})
            CREATE (ws2mNode:WS2M {name: "WS2M", value: $WS2M})
            CREATE (t2mdewNode:T2MDEW {name: "T2MDEW", value: $T2MDEW})
            CREATE (gwettopNode:GWETTOP {name: "GWETTOP", value: $GWETTOP})
            CREATE (cloudAmtNode:CLOUD_AMT {name: "CLOUD_AMT", value: $CLOUD_AMT})
            CREATE (prectotcorrNode:PRECTOTCORR {name: "PRECTOTCORR", value: $PRECTOTCORR})
            CREATE (cropStageNode:CropStage {name: "CropStage", value: $Crop_Stage})
            CREATE (varietyNode:CropVarietyType {name: "CropVarietyType", value: $Crop_Variety_Type})
            CREATE (seasonNode:Season {name: "Season", value: $Season})
            CREATE (durationNode:Duration {name: "Duration", value: $Duration})
            CREATE (dayOfCropNode:DayOfCrop {name: "DayOfCrop", value: $Day_of_Crop})
            CREATE (soilMoistureContentNode:SoilMoistureContent {name: "SoilMoistureContent", value: $Soil_Moisture_Content})
            CREATE (soilTemperatureNode:SoilTemperature {name: "SoilTemperature", value: $Soil_Temperature})
            CREATE (soilPHNode:SoilPH {name: "SoilPH", value: $Soil_pH})
            CREATE (sodiumNode:Sodium {name: "Sodium", value: $Sodium})
            CREATE (nitrogenNode:Nitrogen {name: "Nitrogen", value: $Nitrogen})
            CREATE (magnesiumNode:Magnesium {name: "Magnesium", value: $Magnesium})
            CREATE (boronNode:Boron {name: "Boron", value: $Boron})
            CREATE (chlorineNode:Chlorine {name: "Chlorine", value: $Chlorine})
            CREATE (copperNode:Copper {name: "Copper", value: $Copper})
            CREATE (ironNode:Iron {name: "Iron", value: $Iron})
            CREATE (manganeseNode:Manganese {name: "Manganese", value: $Manganese})
            CREATE (molybdenumNode:Molybdenum {name: "Molybdenum", value: $Molybdenum})
            CREATE (zincNode:Zinc {name: "Zinc", value: $Zinc})
            CREATE (nickelNode:Nickel {name: "Nickel", value: $Nickel})
            CREATE (waterRetentionCapacityNode:Water_Retention_Capacity {name: "Water_Retention_Capacity", value: $Water_Retention_Capacity})
            CREATE (calciumNode:Calcium {name: "Calcium", value: $Calcium})
            CREATE (sulphurNode:Sulphur {name: "Sulphur", value: $Sulphur})
            CREATE (phosphorousNode:Phosphorous {name: "Phosphorous", value: $Phosphorous})
            CREATE (potassiumNode:Potassium {name: "Potassium", value: $Potassium})
            CREATE (cationExchangeCapacityNode:Cation_Exchange_Capacity {name: "Cation_Exchange_Capacity", value: $Cation_Exchange_Capacity})
            CREATE (electricalConductivityNode:Electrical_Conductivity {name: "Electrical_Conductivity", value: $Electrical_Conductivity})
            CREATE (waterRequirementNode:Water_Requirement {name: "Water_Requirement", value: $Water_Requirement})
            CREATE (radiationNode:Radiation {name: "Radiation", value: $Radiation})
            CREATE (ambientTemperatureNode:Ambient_Temperature {name: "Ambient_Temperature", value: $Ambient_Temperature})
            CREATE (humidityNode:Humidity {name: "Humidity", value: $Humidity})
            CREATE (sunlightExposureNode:Sunlight_Exposure {name: "Sunlight_Exposure", value: $Sunlight_Exposure})
            CREATE (rainfallNode:Rainfall {name: "Rainfall", value: $Rainfall})
            CREATE (windSpeedNode:Wind_Speed {name: "Wind_Speed", value: $Wind_Speed})
            CREATE (cloudCoverNode:Cloud_Cover {name: "Cloud_Cover", value: $Cloud_Cover})
            CREATE (radiationExposureNode:Radiation_Exposure {name: "Radiation_Exposure", value: $Radiation_Exposure})
            CREATE (seedVigorIndexNode:Seed_Vigor_Index {name: "Seed_Vigor_Index", value: $Seed_Vigor_Index})
            CREATE (germinationRateNode:Germination_Rate {name: "Germination_Rate", value: $Germination_Rate})
            CREATE (diseaseResistanceNode:Disease_Resistance {name: "Disease_Resistance", value: $Disease_Resistance})
            CREATE (pestResistanceNode:Pest_Resistance {name: "Pest_Resistance", value: $Pest_Resistance})
            CREATE (rootGrowthRateNode:Root_Growth_Rate {name: "Root_Growth_Rate", value: $Root_Growth_Rate})
            CREATE (shootEmergenceTimeNode:Shoot_Emergence_Time {name: "Shoot_Emergence_Time", value: $Shoot_Emergence_Time})
            CREATE (plantingDepthNode:Planting_Depth {name: "Planting_Depth", value: $Planting_Depth})
            CREATE (seedSpacingNode:Seed_Spacing {name: "Seed_Spacing", value: $Seed_Spacing})
            CREATE (vegetationIndexNode:Vegetation_Index {name: "Vegetation_Index", value: $Vegetation_Index})
            CREATE (seedViabilityNode:Seed_Viability {name: "Seed_Viability", value: $Seed_Viability})
            CREATE (seedMoistureContentNode:Seed_Moisture_Content {name: "Seed_Moisture_Content", value: $Seed_Moisture_Content})
            CREATE (initialFertilizerApplicationNode:Initial_Fertilizer_Application {name: "Initial_Fertilizer_Application", value: $Initial_Fertilizer_Application})
            CREATE (heatStressIndexNode:Heat_Stress_Index {name: "Heat_Stress_Index", value: $Heat_Stress_Index})
            CREATE (waterStressIndexNode:Water_Stress_Index {name: "Water_Stress_Index", value: $Water_Stress_Index})
            CREATE (salinityStressIndexNode:Salinity_Stress_Index {name: "Salinity_Stress_Index", value: $Salinity_Stress_Index})
            CREATE (soilRespirationRateNode:Soil_Respiration_Rate {name: "Soil_Respiration_Rate", value: $Soil_Respiration_Rate})
            CREATE (organicMatterContentNode:Organic_Matter_Content {name: "Organic_Matter_Content", value: $Organic_Matter_Content})
            CREATE (yieldNode:Yield {name: "Yield", value: $Yield})

            // Relationships
            CREATE (crop)-[:HAS_DATE]->(dateNode)
            CREATE (dateNode)-[:HAS_TIME_OF_DAY]->(timeNode)
            CREATE (timeNode)-[:HAS_LAT]->(latNode)
            CREATE (latNode)-[:HAS_LON]->(lonNode)
            CREATE (lonNode)-[:HAS_TS]->(tsNode)
            CREATE (tsNode)-[:HAS_T2M]->(t2mNode)
            CREATE (t2mNode)-[:HAS_RH2M]->(rh2mNode)
            CREATE (rh2mNode)-[:HAS_WS2M]->(ws2mNode)
            CREATE (ws2mNode)-[:HAS_T2MDEW]->(t2mdewNode)
            CREATE (t2mdewNode)-[:HAS_GWETTOP]->(gwettopNode)
            CREATE (gwettopNode)-[:HAS_CLOUD_AMT]->(cloudAmtNode)
            CREATE (cloudAmtNode)-[:HAS_PRECTOTCORR]->(prectotcorrNode)
            CREATE (prectotcorrNode)-[:HAS_CROP_STAGE]->(cropStageNode)
            CREATE (cropStageNode)-[:HAS_CROP_VARIETY_TYPE]->(varietyNode)
            CREATE (varietyNode)-[:HAS_SEASON]->(seasonNode)
            CREATE (seasonNode)-[:HAS_DURATION]->(durationNode)
            CREATE (durationNode)-[:HAS_DAY_OF_CROP]->(dayOfCropNode)
            CREATE (dayOfCropNode)-[:HAS_SOIL_MOISTURE_CONTENT]->(soilMoistureContentNode)
            CREATE (soilMoistureContentNode)-[:HAS_SOIL_TEMPERATURE]->(soilTemperatureNode)
            CREATE (soilTemperatureNode)-[:HAS_SOIL_PH]->(soilPHNode)
            CREATE (soilPHNode)-[:HAS_SODIUM]->(sodiumNode)
            CREATE (sodiumNode)-[:HAS_NITROGEN]->(nitrogenNode)
            CREATE (nitrogenNode)-[:HAS_MAGNESIUM]->(magnesiumNode)
            CREATE (magnesiumNode)-[:HAS_BORON]->(boronNode)
            CREATE (boronNode)-[:HAS_CHLORINE]->(chlorineNode)
            CREATE (chlorineNode)-[:HAS_COPPER]->(copperNode)
            CREATE (copperNode)-[:HAS_IRON]->(ironNode)
            CREATE (ironNode)-[:HAS_MANGANESE]->(manganeseNode)
            CREATE (manganeseNode)-[:HAS_MOLYBDENUM]->(molybdenumNode)
            CREATE (molybdenumNode)-[:HAS_ZINC]->(zincNode)
            CREATE (zincNode)-[:HAS_NICKEL]->(nickelNode)
            CREATE (nickelNode)-[:HAS_WATER_RETENTION_CAPACITY]->(waterRetentionCapacityNode)
            CREATE (waterRetentionCapacityNode)-[:HAS_CALCIUM]->(calciumNode)
            CREATE (calciumNode)-[:HAS_SULPHUR]->(sulphurNode)
            CREATE (sulphurNode)-[:HAS_PHOSPHOROUS]->(phosphorousNode)
            CREATE (phosphorousNode)-[:HAS_POTASSIUM]->(potassiumNode)
            CREATE (potassiumNode)-[:HAS_CATION_EXCHANGE_CAPACITY]->(cationExchangeCapacityNode)
            CREATE (cationExchangeCapacityNode)-[:HAS_ELECTRICAL_CONDUCTIVITY]->(electricalConductivityNode)
            CREATE (electricalConductivityNode)-[:HAS_WATER_REQUIREMENT]->(waterRequirementNode)
            CREATE (waterRequirementNode)-[:HAS_RADIATION]->(radiationNode)
            CREATE (radiationNode)-[:HAS_AMBIENT_TEMPERATURE]->(ambientTemperatureNode)
            CREATE (ambientTemperatureNode)-[:HAS_HUMIDITY]->(humidityNode)
            CREATE (humidityNode)-[:HAS_SUNLIGHT_EXPOSURE]->(sunlightExposureNode)
            CREATE (sunlightExposureNode)-[:HAS_RAINFALL]->(rainfallNode)
            CREATE (rainfallNode)-[:HAS_WIND_SPEED]->(windSpeedNode)
            CREATE (windSpeedNode)-[:HAS_CLOUD_COVER]->(cloudCoverNode)
            CREATE (cloudCoverNode)-[:HAS_RADIATION_EXPOSURE]->(radiationExposureNode)
            CREATE (radiationExposureNode)-[:HAS_SEED_VIGOR_INDEX]->(seedVigorIndexNode)
            CREATE (seedVigorIndexNode)-[:HAS_GERMINATION_RATE]->(germinationRateNode)
            CREATE (germinationRateNode)-[:HAS_DISEASE_RESISTANCE]->(diseaseResistanceNode)
            CREATE (diseaseResistanceNode)-[:HAS_PEST_RESISTANCE]->(pestResistanceNode)
            CREATE (pestResistanceNode)-[:HAS_ROOT_GROWTH_RATE]->(rootGrowthRateNode)
            CREATE (rootGrowthRateNode)-[:HAS_SHOOT_EMERGENCE_TIME]->(shootEmergenceTimeNode)
            CREATE (shootEmergenceTimeNode)-[:HAS_PLANTING_DEPTH]->(plantingDepthNode)
            CREATE (plantingDepthNode)-[:HAS_SEED_SPACING]->(seedSpacingNode)
            CREATE (seedSpacingNode)-[:HAS_VEGETATION_INDEX]->(vegetationIndexNode)
            CREATE (vegetationIndexNode)-[:HAS_SEED_VIABILITY]->(seedViabilityNode)
            CREATE (seedViabilityNode)-[:HAS_SEED_MOISTURE_CONTENT]->(seedMoistureContentNode)
            CREATE (seedMoistureContentNode)-[:HAS_INITIAL_FERTILIZER_APPLICATION]->(initialFertilizerApplicationNode)
            CREATE (initialFertilizerApplicationNode)-[:HAS_HEAT_STRESS_INDEX]->(heatStressIndexNode)
            CREATE (heatStressIndexNode)-[:HAS_WATER_STRESS_INDEX]->(waterStressIndexNode)
            CREATE (waterStressIndexNode)-[:HAS_SALINITY_STRESS_INDEX]->(salinityStressIndexNode)
            CREATE (salinityStressIndexNode)-[:HAS_SOIL_RESPIRATION_RATE]->(soilRespirationRateNode)
            CREATE (soilRespirationRateNode)-[:HAS_ORGANIC_MATTER_CONTENT]->(organicMatterContentNode)
            CREATE (organicMatterContentNode)-[:HAS_YIELD]->(yieldNode)
            """

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
                "Sodium": row["Sodium"],
                "Nitrogen": row["Nitrogen"],
                "Magnesium": row["Magnesium"],
                "Boron": row["Boron"],
                "Chlorine": row["Chlorine"],
                "Copper": row["Copper"],
                "Iron": row["Iron"],
                "Manganese": row["Manganese"],
                "Molybdenum": row["Molybdenum"],
                "Zinc": row["Zinc"],
                "Nickel": row["Nickel"],
                "Water_Retention_Capacity": row["Water_Retention_Capacity"],
                "Calcium": row["Calcium"],
                "Sulphur": row["Sulphur"],
                "Phosphorous": row["Phosphorous"],
                "Potassium": row["Potassium"],
                "Cation_Exchange_Capacity": row["Cation_Exchange_Capacity"],
                "Electrical_Conductivity": row["Electrical_Conductivity"],
                "Water_Requirement": row["Water_Requirement"],
                "Radiation": row["Radiation"],
                "Ambient_Temperature": row["Ambient_Temperature"],
                "Humidity": row["Humidity"],
                "Sunlight_Exposure": row["Sunlight_Exposure"],
                "Rainfall": row["Rainfall"],
                "Wind_Speed": row["Wind_Speed"],
                "Cloud_Cover": row["Cloud_Cover"],
                "Radiation_Exposure": row["Radiation_Exposure"],
                "Seed_Vigor_Index": row["Seed_Vigor_Index"],
                "Germination_Rate": row["Germination_Rate"],
                "Disease_Resistance": row["Disease_Resistance"],
                "Pest_Resistance": row["Pest_Resistance"],
                "Root_Growth_Rate": row["Root_Growth_Rate"],
                "Shoot_Emergence_Time": row["Shoot_Emergence_Time"],
                "Planting_Depth": row["Planting_Depth"],
                "Seed_Spacing": row["Seed_Spacing"],
                "Vegetation_Index": row["Vegetation_Index"],
                "Seed_Viability": row["Seed_Viability"],
                "Seed_Moisture_Content": row["Seed_Moisture_Content"],
                "Initial_Fertilizer_Application": row["Initial_Fertilizer_Application"],
                "Heat_Stress_Index": row["Heat_Stress_Index"],
                "Water_Stress_Index": row["Water_Stress_Index"],
                "Salinity_Stress_Index": row["Salinity_Stress_Index"],
                "Soil_Respiration_Rate": row["Soil_Respiration_Rate"],
                "Organic_Matter_Content": row["Organic_Matter_Content"],
                "Yield": row["Yield"],
            }

            # Execute the query
            neo4j_connection.execute_query(query, params)

        print("Nodes and relationships successfully created in Neo4j!")
    except Exception as e:
        print(f"Error creating nodes and relationships in Neo4j: {e}")

def export_germination_relationships_from_neo4j(neo4j_connection, output_file):
    try:
        # Define the query to export data in the required order
        export_query = """
        MATCH (crop:Crop {name: 'Rice'})-[:HAS_DATE]->(date:Date)
OPTIONAL MATCH (date)-[:HAS_TIME_OF_DAY]->(time:TimeOfDay)
OPTIONAL MATCH (time)-[:HAS_LAT]->(lat:LAT)
OPTIONAL MATCH (lat)-[:HAS_LON]->(lon:LON)
OPTIONAL MATCH (lon)-[:HAS_TS]->(ts:TS)
OPTIONAL MATCH (ts)-[:HAS_T2M]->(t2m:T2M)
OPTIONAL MATCH (t2m)-[:HAS_RH2M]->(rh2m:RH2M)
OPTIONAL MATCH (rh2m)-[:HAS_WS2M]->(ws2m:WS2M)
OPTIONAL MATCH (ws2m)-[:HAS_T2MDEW]->(t2mdew:T2MDEW)
OPTIONAL MATCH (t2mdew)-[:HAS_GWETTOP]->(gwettop:GWETTOP)
OPTIONAL MATCH (gwettop)-[:HAS_CLOUD_AMT]->(cloudAmt:CLOUD_AMT)
OPTIONAL MATCH (cloudAmt)-[:HAS_PRECTOTCORR]->(prectotcorr:PRECTOTCORR)
OPTIONAL MATCH (prectotcorr)-[:HAS_CROP_STAGE]->(cropStage:CropStage)
OPTIONAL MATCH (cropStage)-[:HAS_CROP_VARIETY_TYPE]->(variety:CropVarietyType)
OPTIONAL MATCH (variety)-[:HAS_SEASON]->(season:Season)
OPTIONAL MATCH (season)-[:HAS_DURATION]->(duration:Duration)
OPTIONAL MATCH (duration)-[:HAS_DAY_OF_CROP]->(dayOfCrop:DayOfCrop)
OPTIONAL MATCH (dayOfCrop)-[:HAS_SOIL_MOISTURE_CONTENT]->(soilMoistureContent:SoilMoistureContent)
OPTIONAL MATCH (soilMoistureContent)-[:HAS_SOIL_TEMPERATURE]->(soilTemperature:SoilTemperature)
OPTIONAL MATCH (soilTemperature)-[:HAS_SOIL_PH]->(soilPH:SoilPH)
OPTIONAL MATCH (soilPH)-[:HAS_SODIUM]->(sodium:Sodium)
OPTIONAL MATCH (sodium)-[:HAS_NITROGEN]->(nitrogen:Nitrogen)
OPTIONAL MATCH (nitrogen)-[:HAS_MAGNESIUM]->(magnesium:Magnesium)
OPTIONAL MATCH (magnesium)-[:HAS_BORON]->(boron:Boron)
OPTIONAL MATCH (boron)-[:HAS_CHLORINE]->(chlorine:Chlorine)
OPTIONAL MATCH (chlorine)-[:HAS_COPPER]->(copper:Copper)
OPTIONAL MATCH (copper)-[:HAS_IRON]->(iron:Iron)
OPTIONAL MATCH (iron)-[:HAS_MANGANESE]->(manganese:Manganese)
OPTIONAL MATCH (manganese)-[:HAS_MOLYBDENUM]->(molybdenum:Molybdenum)
OPTIONAL MATCH (molybdenum)-[:HAS_ZINC]->(zinc:Zinc)
OPTIONAL MATCH (zinc)-[:HAS_NICKEL]->(nickel:Nickel)
OPTIONAL MATCH (nickel)-[:HAS_WATER_RETENTION_CAPACITY]->(waterRetentionCapacity:Water_Retention_Capacity)
OPTIONAL MATCH (waterRetentionCapacity)-[:HAS_CALCIUM]->(calcium:Calcium)
OPTIONAL MATCH (calcium)-[:HAS_SULPHUR]->(sulphur:Sulphur)
OPTIONAL MATCH (sulphur)-[:HAS_PHOSPHOROUS]->(phosphorous:Phosphorous)
OPTIONAL MATCH (phosphorous)-[:HAS_POTASSIUM]->(potassium:Potassium)
OPTIONAL MATCH (potassium)-[:HAS_CATION_EXCHANGE_CAPACITY]->(cationExchangeCapacity:Cation_Exchange_Capacity)
OPTIONAL MATCH (cationExchangeCapacity)-[:HAS_ELECTRICAL_CONDUCTIVITY]->(electricalConductivity:Electrical_Conductivity)
OPTIONAL MATCH (electricalConductivity)-[:HAS_WATER_REQUIREMENT]->(waterRequirement:Water_Requirement)
OPTIONAL MATCH (waterRequirement)-[:HAS_RADIATION]->(radiation:Radiation)
OPTIONAL MATCH (radiation)-[:HAS_AMBIENT_TEMPERATURE]->(ambientTemperature:Ambient_Temperature)
OPTIONAL MATCH (ambientTemperature)-[:HAS_HUMIDITY]->(humidity:Humidity)
OPTIONAL MATCH (humidity)-[:HAS_SUNLIGHT_EXPOSURE]->(sunlightExposure:Sunlight_Exposure)
OPTIONAL MATCH (sunlightExposure)-[:HAS_RAINFALL]->(rainfall:Rainfall)
OPTIONAL MATCH (rainfall)-[:HAS_WIND_SPEED]->(windSpeed:Wind_Speed)
OPTIONAL MATCH (windSpeed)-[:HAS_CLOUD_COVER]->(cloudCover:Cloud_Cover)
OPTIONAL MATCH (cloudCover)-[:HAS_RADIATION_EXPOSURE]->(radiationExposure:Radiation_Exposure)
OPTIONAL MATCH (radiationExposure)-[:HAS_SEED_VIGOR_INDEX]->(seedVigorIndex:Seed_Vigor_Index)
OPTIONAL MATCH (seedVigorIndex)-[:HAS_GERMINATION_RATE]->(germinationRate:Germination_Rate)
OPTIONAL MATCH (germinationRate)-[:HAS_DISEASE_RESISTANCE]->(diseaseResistance:Disease_Resistance)
OPTIONAL MATCH (diseaseResistance)-[:HAS_PEST_RESISTANCE]->(pestResistance:Pest_Resistance)
OPTIONAL MATCH (pestResistance)-[:HAS_ROOT_GROWTH_RATE]->(rootGrowthRate:Root_Growth_Rate)
OPTIONAL MATCH (rootGrowthRate)-[:HAS_SHOOT_EMERGENCE_TIME]->(shootEmergenceTime:Shoot_Emergence_Time)
OPTIONAL MATCH (shootEmergenceTime)-[:HAS_PLANTING_DEPTH]->(plantingDepth:Planting_Depth)
OPTIONAL MATCH (plantingDepth)-[:HAS_SEED_SPACING]->(seedSpacing:Seed_Spacing)
OPTIONAL MATCH (seedSpacing)-[:HAS_VEGETATION_INDEX]->(vegetationIndex:Vegetation_Index)
OPTIONAL MATCH (vegetationIndex)-[:HAS_SEED_VIABILITY]->(seedViability:Seed_Viability)
OPTIONAL MATCH (seedViability)-[:HAS_SEED_MOISTURE_CONTENT]->(seedMoistureContent:Seed_Moisture_Content)
OPTIONAL MATCH (seedMoistureContent)-[:HAS_INITIAL_FERTILIZER_APPLICATION]->(initialFertilizerApplication:Initial_Fertilizer_Application)
OPTIONAL MATCH (initialFertilizerApplication)-[:HAS_HEAT_STRESS_INDEX]->(heatStressIndex:Heat_Stress_Index)
OPTIONAL MATCH (heatStressIndex)-[:HAS_WATER_STRESS_INDEX]->(waterStressIndex:Water_Stress_Index)
OPTIONAL MATCH (waterStressIndex)-[:HAS_SALINITY_STRESS_INDEX]->(salinityStressIndex:Salinity_Stress_Index)
OPTIONAL MATCH (salinityStressIndex)-[:HAS_SOIL_RESPIRATION_RATE]->(soilRespirationRate:Soil_Respiration_Rate)
OPTIONAL MATCH (soilRespirationRate)-[:HAS_ORGANIC_MATTER_CONTENT]->(organicMatterContent:Organic_Matter_Content)
OPTIONAL MATCH (organicMatterContent)-[:HAS_YIELD]->(yieldNode:Yield)
RETURN 
    date.value AS Date,
    time.value AS TimeOfDay,
    lat.value AS LAT,
    lon.value AS LON,
    ts.value AS TS,
    t2m.value AS T2M,
    rh2m.value AS RH2M,
    ws2m.value AS WS2M,
    t2mdew.value AS T2MDEW,
    gwettop.value AS GWETTOP,
    cloudAmt.value AS CLOUD_AMT,
    prectotcorr.value AS PRECTOTCORR,
    cropStage.value AS Crop_Stage,
    variety.value AS Crop_Variety_Type,
    season.value AS Season,
    duration.value AS Duration,
    dayOfCrop.value AS Day_of_Crop,
    soilMoistureContent.value AS Soil_Moisture_Content,
    soilTemperature.value AS Soil_Temperature,
    soilPH.value AS Soil_pH,
    sodium.value AS Sodium,
    nitrogen.value AS Nitrogen,
    magnesium.value AS Magnesium,
    boron.value AS Boron,
    chlorine.value AS Chlorine,
    copper.value AS Copper,
    iron.value AS Iron,
    manganese.value AS Manganese,
    molybdenum.value AS Molybdenum,
    zinc.value AS Zinc,
    nickel.value AS Nickel,
    waterRetentionCapacity.value AS Water_Retention_Capacity,
    calcium.value AS Calcium,
    sulphur.value AS Sulphur,
    phosphorous.value AS Phosphorous,
    potassium.value AS Potassium,
    cationExchangeCapacity.value AS Cation_Exchange_Capacity,
    electricalConductivity.value AS Electrical_Conductivity,
    waterRequirement.value AS Water_Requirement,
    radiation.value AS Radiation,
    ambientTemperature.value AS Ambient_Temperature,
    humidity.value AS Humidity,
    sunlightExposure.value AS Sunlight_Exposure,
    rainfall.value AS Rainfall,
    windSpeed.value AS Wind_Speed,
    cloudCover.value AS Cloud_Cover,
    radiationExposure.value AS Radiation_Exposure,
    seedVigorIndex.value AS Seed_Vigor_Index,
    germinationRate.value AS Germination_Rate,
    diseaseResistance.value AS Disease_Resistance,
    pestResistance.value AS Pest_Resistance,
    rootGrowthRate.value AS Root_Growth_Rate,
    shootEmergenceTime.value AS Shoot_Emergence_Time,
    plantingDepth.value AS Planting_Depth,
    seedSpacing.value AS Seed_Spacing,
    vegetationIndex.value AS Vegetation_Index,
    seedViability.value AS Seed_Viability,
    seedMoistureContent.value AS Seed_Moisture_Content,
    initialFertilizerApplication.value AS Initial_Fertilizer_Application,
    heatStressIndex.value AS Heat_Stress_Index,
    waterStressIndex.value AS Water_Stress_Index,
    salinityStressIndex.value AS Salinity_Stress_Index,
    soilRespirationRate.value AS Soil_Respiration_Rate,
    organicMatterContent.value AS Organic_Matter_Content,
    yieldNode.value AS Yield

        """
        # Execute the query and fetch the results
        results = neo4j_connection.execute_query(export_query)
        print("Query Results:", results)  # Debugging output

        # Check if results are empty
        if not results:
            print("No data retrieved from Neo4j.")
            return

        # Convert results to a DataFrame
        df = pd.DataFrame(results)
        # print("DataFrame preview:\n", df.head())  # Debugging output

        # Save DataFrame to a CSV file
        df.to_csv(output_file, index=False)
        print(f"Data successfully exported to {output_file}")
    except Exception as e:
        print(f"Error exporting data from Neo4j: {e}")
       
uri = "bolt://localhost:7687"
user = "neo4j"  
password = "Naveen1@"
db_name="Germination"
neo4j_conn = Neo4jConnection(uri, user, password,db_name="Germination")
create_germination_relationships_in_neo4j(merged_df,neo4j_conn)
export_germination_relationships_from_neo4j(neo4j_conn, "germination_stage_export.csv")
neo4j_conn.close()