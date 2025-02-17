import random
import pandas as pd
from datetime import datetime, timedelta
from neo4j import GraphDatabase
import random
import pandas as pd
from datetime import datetime, timedelta


class Neo4jConnection:
    def __init__(self, uri, user, password, db_name="Trilling"):
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
def generate_tillering_stage_dataset_with_yield():
    try:
        # Input date range from user
        start_date_input = input("Enter the start date (YYYY-MM-DD): ")
        end_date_input = input("Enter the end date (YYYY-MM-DD): ")

        start_date = datetime.strptime(start_date_input, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_input, "%Y-%m-%d")

        if start_date > end_date:
            raise ValueError("Start date must be earlier than end date.")
        
        # Define tillering parameters
        Tillering_parameters = {
            "Boron": (0.3, 1),
            "Soil_Ph": (5.5, 7),
            "Molybdenum": (0.01, 0.05),
            "Chlorine": (0.2, 2.0),
            "Nickel": (0.01, 0.1),
            "Sulphur": (0.5, 1.0),
            "Phosphorus": (15, 25),
            "Potassium": (100, 200),
            "Sodium": (100, 200),
            "Nitrogen": (10, 20),
            "Calcium": (15, 30),
            "Sulfur": (15, 30),
            "Phosphorous": (2, 3),
            "Zinc": (0.5, 2),
            "Copper": (0.2, 0.5),
            "Manganese": (10, 50),
            "Leaf_Initiation_Plastochron": (3, 5),
            "Leaf_Elongation": (1.0, 1.5),
            "Radiation": (300, 500),
            "Iron": (4, 10),
            "Radiation_Exposure":(300,500),
            "Magnesium":(300,500),
            "Leaf_Expansion": (20, 50),
            "Leaf_Length": (20, 50),
            "Leaf_Width": (1.0, 1.5),
            "Leaf_Count": (12, 18),
            "Water_Requirement": (2.5, 5),
            "Lower_Leaf_Length": (15, 25),
            "Vegetation_Leaf_Area_Index": (2.5, 4.5),
            "Vegetation_Chlorophyll_Content": (35, 45),
            "Vegetation_Water_Stress_Index": (0, 0.5),
            "Vegetation_Soil_Organic_Matter": (1.5, 2),
            "Vegetation_Cloud_Cover": (0, 20),
            "Vegetation_Temperature_Range": (25, 30),
            "Lower_Leaf_Width": (0.8, 1.2),
            "Middle_Leaf_Length": (30, 50),
            "Middle_Leaf_Width": (1.0, 1.5),
            "Soil_Sensitivity_Highly": (12, 14),
            "Soil_Sensitivity_Moderate": (14, 16),
            "Light_Intensity_Low": (0, 2),
            "Light_Intensity_Moderate": (12, 14),
            "Flood_Depth_Ideal": (5, 10),
            "Humidity_Moderate": (50, 70),
            "Humidity_High": (70, 90),
        }

        # Define crop varieties and durations
        data = []
        current_date = start_date
        tillering_cycle_start = 21
        tillering_cycle_end = 32
        day_of_tillering = tillering_cycle_start
        times_of_day = ['morning', 'afternoon', 'evening']

        # Generate data for each day
        while current_date <= end_date:
            for time_of_day in times_of_day:
                row = {
                    'Crop_Stage': "Tillering",
                    'Date': current_date.strftime('%Y-%m-%d'),
                    'TimeOfDay': time_of_day,
                    'Rice_Crop_Variety': random.choice(['Long', 'Medium', 'Short']),
                    "Season": random.choice(["Rabi", "Kharif"]),
                    "Duration": random.randint(90, 140),
                    "Day_of_Tillering": day_of_tillering,
                    "Yield": round(random.uniform(0, 100), 2),
                }

                # Randomize parameters
                for param, (min_val, max_val) in Tillering_parameters.items():
                    if isinstance(min_val, float) or isinstance(max_val, float):
                        row[param] = round(random.uniform(min_val, max_val), 2)
                    else:
                        row[param] = random.randint(min_val, max_val)

                data.append(row)

            # Move to the next day
            current_date += timedelta(days=1)

            # Increment day_of_tillering and reset if needed
            day_of_tillering += 1
            if day_of_tillering > tillering_cycle_end:
                day_of_tillering = tillering_cycle_start

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
        sowing_df["Year"] = pd.to_datetime(sowing_df["Date"], format="%Y-%m-%d").dt.year
        sowing_df["Month"] = pd.to_datetime(sowing_df["Date"], format="%Y-%m-%d").dt.strftime("%b").str.upper()

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


Trilling_df = generate_tillering_stage_dataset_with_yield()
power_df = pd.read_csv("power.csv")
merged_df = merge_power_data(Trilling_df, power_df)
# merged_df.to_csv("Trilling_stage_dataset_last.csv", index=False)
# print("Dataset generated and saved to 'Trilling_stage_dataset_last'.")

desired_columns = [
    "Date", "TimeOfDay","LAT","LON","TS","T2M", "RH2M", "WS2M", "T2MDEW", "GWETTOP",
    "CLOUD_AMT", "PRECTOTCORR",
    "Crop_Stage", "Rice_Crop_Variety", 
    "Season", "Duration", "Day_of_Tillering","Boron",
    "Soil_Ph",
    "Molybdenum",
    "Chlorine",
    "Nickel",
    "Sulphur",     
    "Phosphorus",   
    "Potassium",
    "Sodium",
    "Nitrogen",
    "Calcium",
    "Zinc",
    "Copper",
    "Manganese",
    "Iron",
    "Water_Requirement",
    "Radiation_Exposure",
    "Magnesium",
    "Leaf_Initiation_Plastochron",
    "Leaf_Elongation",
    "Radiation",
    "Leaf_Expansion",
    "Leaf_Length",
    "Leaf_Width",
    "Leaf_Count",
    "Lower_Leaf_Length",
    "Lower_Leaf_Width",
    "Middle_Leaf_Length",
    "Middle_Leaf_Width",
    "Vegetation_Leaf_Area_Index",
    "Vegetation_Chlorophyll_Content",
    "Vegetation_Water_Stress_Index",
    "Vegetation_Soil_Organic_Matter",
    "Vegetation_Cloud_Cover",
    "Vegetation_Temperature_Range",
    "Soil_Sensitivity_Highly",
    "Soil_Sensitivity_Moderate",
    "Light_Intensity_Low",
    "Light_Intensity_Moderate",
    "Flood_Depth_Ideal",
    "Humidity_Moderate",
    "Humidity_High",
    "Yield"
]


merged_df = merged_df.reindex(columns=desired_columns)

# merged_df.to_csv("Trilling_stage_dataset_last_reindex.csv", index=False)

def create_tillering_relationships_in_neo4j(merged_df, neo4j_connection):
    try:
        # Ensure the base "Rice" node exists
        neo4j_connection.execute_query("MERGE (rice:Crop {name: 'Rice'})")

        for _, row in merged_df.iterrows():
            query = """
            
            MATCH (rice:Crop {name: 'Rice'})

            WITH rice
            // Date / Time / Crop Stage
            CREATE (date:Date {name: 'Date', value: $Date})
            CREATE (timeOfDay:TimeOfDay {name: 'TimeOfDay', value: $TimeOfDay})
            CREATE (crop_stage:CropStage {name: 'Crop Stage', value: $Crop_Stage})
            
            // Meteorological / Environmental Metrics
            CREATE (LAT:LAT {name: 'LAT', value: $LAT})
            CREATE (LON:LON {name: 'LON', value: $LON})
            CREATE (ts:TS {name: 'Temperature Surface', value: $TS})
            CREATE (t2m:T2M {name: 'Temperature 2M', value: $T2M})
            CREATE (rh2m:RH2M {name: 'Relative Humidity 2M', value: $RH2M})
            CREATE (ws2m:WS2M {name: 'Wind Speed 2M', value: $WS2M})
            CREATE (t2mdew:T2MDEW {name: 'Dew Point Temperature 2M', value: $T2MDEW})
            CREATE (gwettop:GWETTOP {name: 'Ground Wetness Top Layer', value: $GWETTOP})
            CREATE (cloud_amt:CLOUD_AMT {name: 'Cloud Amount', value: $CLOUD_AMT})
            CREATE (prectotcorr:PRECTOTCORR {name: 'Corrected Precipitation', value: $PRECTOTCORR})
            
            // Crop Info
            CREATE (variety:RiceVariety {name: 'Rice Crop Variety', value: $Rice_Crop_Variety})
            CREATE (season:Season {name: 'Season', value: $Season})
            CREATE (duration:Duration {name: 'Duration', value: $Duration})
            CREATE (dayOfTillering:DayOfTillering {name: 'Day of Tillering', value: $Day_of_Tillering})
            
            // Nutrient / Soil / Water
            CREATE (boron:Boron {name: 'Boron', value: $Boron})
            CREATE (soil_Ph:Soil_Ph {name: 'Soil Ph', value: $Soil_Ph})
            CREATE (molybdenum:Molybdenum {name: 'Molybdenum', value: $Molybdenum})
            CREATE (chlorine:Chlorine {name: 'Chlorine', value: $Chlorine})
            CREATE (nickel:Nickel {name: 'Nickel', value: $Nickel})
            CREATE (Sulphur:Sulphur {name: 'Sulphur', value: $Sulphur})
            CREATE (phosphorus:Phosphorus {name: 'Phosphorus', value: $Phosphorus})
            CREATE (potassium:Potassium {name: 'Potassium', value: $Potassium})
            CREATE (sodium:Sodium {name: 'Sodium', value: $Sodium})
            CREATE (nitrogen:Nitrogen {name: 'Nitrogen', value: $Nitrogen})
            CREATE (calcium:Calcium {name: 'Calcium', value: $Calcium})
            CREATE (zinc:Zinc {name: 'Zinc', value: $Zinc})
            CREATE (copper:Copper {name: 'Copper', value: $Copper})
            CREATE (manganese:Manganese {name: 'Manganese', value: $Manganese})
            CREATE (iron:Iron {name: 'Iron', value: $Iron})
            CREATE (Water_Requirement:Water_Requirement {name: 'Water Requirement', value: $Water_Requirement})
            CREATE (Radiation_Exposure:Radiation_Exposure {name: 'Radiation Exposure', value: $Radiation_Exposure})
            CREATE (Magnesium:Magnesium {name: 'Magnesium', value: $Magnesium})
            
            // Leaf / Growth
            CREATE (leaf_initiation:LeafInitiationPlastochron {name: 'Leaf Initiation Plastochron', value: $Leaf_Initiation_Plastochron})
            CREATE (leaf_elongation:LeafElongation {name: 'Leaf Elongation', value: $Leaf_Elongation})
            CREATE (radiation:Radiation {name: 'Radiation', value: $Radiation})
            CREATE (leaf_expansion:LeafExpansion {name: 'Leaf Expansion', value: $Leaf_Expansion})
            CREATE (leaf_length:LeafLength {name: 'Leaf Length', value: $Leaf_Length})
            CREATE (leaf_width:LeafWidth {name: 'Leaf Width', value: $Leaf_Width})
            CREATE (leaf_count:LeafCount {name: 'Leaf Count', value: $Leaf_Count})
            CREATE (lower_leaf_length:LowerLeafLength {name: 'Lower Leaf Length', value: $Lower_Leaf_Length})
            CREATE (lower_leaf_width:LowerLeafWidth {name: 'Lower Leaf Width', value: $Lower_Leaf_Width})
            CREATE (middle_leaf_length:MiddleLeafLength {name: 'Middle Leaf Length', value: $Middle_Leaf_Length})
            CREATE (middle_leaf_width:MiddleLeafWidth {name: 'Middle Leaf Width', value: $Middle_Leaf_Width})
            
            // Vegetation
            CREATE (veg_leaf_area:VegetationLeafAreaIndex {name: 'Vegetation Leaf Area Index', value: $Vegetation_Leaf_Area_Index})
            CREATE (veg_chlorophyll:VegetationChlorophyllContent {name: 'Vegetation Chlorophyll Content', value: $Vegetation_Chlorophyll_Content})
            CREATE (veg_water_stress:VegetationWaterStressIndex {name: 'Vegetation Water Stress Index', value: $Vegetation_Water_Stress_Index})
            CREATE (veg_soil_organic:VegetationSoilOrganicMatter {name: 'Vegetation Soil Organic Matter', value: $Vegetation_Soil_Organic_Matter})
            CREATE (veg_cloud_cover:VegetationCloudCover {name: 'Vegetation Cloud Cover', value: $Vegetation_Cloud_Cover})
            CREATE (veg_temp_range:VegetationTemperatureRange {name: 'Vegetation Temperature Range', value: $Vegetation_Temperature_Range})
            
            // Soil / Light / Flood / Humidity
            CREATE (soil_sensitivity_high:SoilSensitivityHighly {name: 'Soil Sensitivity Highly', value: $Soil_Sensitivity_Highly})
            CREATE (soil_sensitivity_mod:SoilSensitivityModerate {name: 'Soil Sensitivity Moderate', value: $Soil_Sensitivity_Moderate})
            CREATE (light_intensity_low:LightIntensityLow {name: 'Light Intensity Low', value: $Light_Intensity_Low})
            CREATE (light_intensity_mod:LightIntensityModerate {name: 'Light Intensity Moderate', value: $Light_Intensity_Moderate})
            CREATE (flood_depth:FloodDepthIdeal {name: 'Flood Depth Ideal', value: $Flood_Depth_Ideal})
            CREATE (humidity_mod:HumidityModerate {name: 'Humidity Moderate', value: $Humidity_Moderate})
            CREATE (humidity_high:HumidityHigh {name: 'Humidity High', value: $Humidity_High})
            
            // Yield
            CREATE (yield_value:Yield {name: 'Yield', value: $Yield})
            
                        // Base Relationships
            CREATE (rice)-[:HAS_DATE]->(date)
            CREATE (date)-[:HAS_TIME_OF_DAY]->(timeOfDay)
            CREATE (timeOfDay)-[:HAS_CROP_STAGE]->(crop_stage)
            
            // Weather / Meteorological Relationships
            CREATE (crop_stage)-[:HAS_METRICS]->(LAT)
            CREATE (LAT)-[:HAS_METRICS]->(LON)
            CREATE (LON)-[:HAS_METRICS]->(ts)
            CREATE (ts)-[:HAS_METRICS]->(t2m)
            CREATE (t2m)-[:HAS_METRICS]->(rh2m)
            CREATE (rh2m)-[:HAS_METRICS]->(ws2m)
            CREATE (ws2m)-[:HAS_METRICS]->(t2mdew)
            CREATE (t2mdew)-[:HAS_METRICS]->(gwettop)
            CREATE (gwettop)-[:HAS_METRICS]->(cloud_amt)
            CREATE (cloud_amt)-[:HAS_METRICS]->(prectotcorr)
            
            // Crop Info Relationships
            CREATE (crop_stage)-[:HAS_VARIETY]->(variety)
            CREATE (variety)-[:HAS_SEASON]->(season)
            CREATE (season)-[:HAS_DURATION]->(duration)
            CREATE (duration)-[:HAS_DAY_OF_TILLERING]->(dayOfTillering)
            
            // Nutrient Relationships
            CREATE (dayOfTillering)-[:HAS_PARAMETERS]->(boron)
            CREATE (boron)-[:HAS_PARAMETERS]->(soil_Ph)
            CREATE (soil_Ph)-[:HAS_PARAMETERS]->(molybdenum)
            CREATE (molybdenum)-[:HAS_PARAMETERS]->(chlorine)
            CREATE (chlorine)-[:HAS_PARAMETERS]->(nickel)
            CREATE (nickel)-[:HAS_PARAMETERS]->(Sulphur)
            CREATE (Sulphur)-[:HAS_PARAMETERS]->(phosphorus)
            CREATE (phosphorus)-[:HAS_PARAMETERS]->(potassium)
            CREATE (potassium)-[:HAS_PARAMETERS]->(sodium)
            CREATE (sodium)-[:HAS_PARAMETERS]->(nitrogen)
            CREATE (nitrogen)-[:HAS_PARAMETERS]->(calcium)
            CREATE (calcium)-[:HAS_PARAMETERS]->(zinc)
            CREATE (zinc)-[:HAS_PARAMETERS]->(copper)
            CREATE (copper)-[:HAS_PARAMETERS]->(manganese)
            CREATE (manganese)-[:HAS_PARAMETERS]->(iron)
            CREATE (iron)-[:HAS_PARAMETERS]->(Water_Requirement)
            CREATE (Water_Requirement)-[:HAS_PARAMETERS]->(Radiation_Exposure)
            CREATE (Radiation_Exposure)-[:HAS_PARAMETERS]->(Magnesium)
            
            // Leaf / Growth Relationships
            CREATE (Magnesium)-[:HAS_LEAF_INFO]->(leaf_initiation)
            CREATE (leaf_initiation)-[:HAS_LEAF_INFO]->(leaf_elongation)
            CREATE (leaf_elongation)-[:HAS_RADIATION]->(radiation)
            CREATE (radiation)-[:HAS_LEAF_INFO]->(leaf_expansion)
            CREATE (leaf_expansion)-[:HAS_DIMENSIONS]->(leaf_length)
            CREATE (leaf_length)-[:HAS_DIMENSIONS]->(leaf_width)
            CREATE (leaf_width)-[:HAS_COUNT]->(leaf_count)
            CREATE (leaf_count)-[:HAS_DIMENSIONS]->(lower_leaf_length)
            CREATE (lower_leaf_length)-[:HAS_DIMENSIONS]->(lower_leaf_width)
            CREATE (lower_leaf_width)-[:HAS_DIMENSIONS]->(middle_leaf_length)
            CREATE (middle_leaf_length)-[:HAS_DIMENSIONS]->(middle_leaf_width)
            
            // Vegetation Relationships
            CREATE (middle_leaf_width)-[:HAS_VEGETATION]->(veg_leaf_area)
            CREATE (veg_leaf_area)-[:HAS_CHLOROPHYLL]->(veg_chlorophyll)
            CREATE (veg_chlorophyll)-[:HAS_WATER_STRESS]->(veg_water_stress)
            CREATE (veg_water_stress)-[:HAS_SOIL]->(veg_soil_organic)
            CREATE (veg_soil_organic)-[:HAS_CLOUD]->(veg_cloud_cover)
            CREATE (veg_cloud_cover)-[:HAS_TEMPERATURE]->(veg_temp_range)
            
            // Soil / Light / Flood / Humidity Relationships
            CREATE (veg_temp_range)-[:HAS_SOIL_SENSITIVITY]->(soil_sensitivity_high)
            CREATE (soil_sensitivity_high)-[:HAS_SOIL_SENSITIVITY]->(soil_sensitivity_mod)
            CREATE (soil_sensitivity_mod)-[:HAS_LIGHT_INTENSITY]->(light_intensity_low)
            CREATE (light_intensity_low)-[:HAS_LIGHT_INTENSITY]->(light_intensity_mod)
            CREATE (light_intensity_mod)-[:HAS_FLOOD_DEPTH]->(flood_depth)
            CREATE (flood_depth)-[:HAS_HUMIDITY]->(humidity_mod)
            CREATE (humidity_mod)-[:HAS_HUMIDITY]->(humidity_high)
            
            // Yield Relationships
            CREATE (humidity_high)-[:HAS_YIELD]->(yield_value)

            """

            params = {
    "Date": row["Date"],
    "TimeOfDay": row["TimeOfDay"],
    "Crop_Stage": row["Crop_Stage"],

    # Weather / Meteorological
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

    # Crop Info
    "Rice_Crop_Variety": row["Rice_Crop_Variety"],
    "Season": row["Season"],
    "Duration": row["Duration"],
    "Day_of_Tillering": row["Day_of_Tillering"],

    # Nutrient / Soil / Water
    "Boron": row["Boron"],
    "Soil_Ph": row["Soil_Ph"],
    "Molybdenum": row["Molybdenum"],
    "Chlorine": row["Chlorine"],
    "Nickel": row["Nickel"],
    "Sulphur": row["Sulphur"],
    "Phosphorus": row["Phosphorus"],
    "Potassium": row["Potassium"],
    "Sodium": row["Sodium"],
    "Nitrogen": row["Nitrogen"],
    "Calcium": row["Calcium"],
    "Zinc": row["Zinc"],
    "Copper": row["Copper"],
    "Manganese": row["Manganese"],
    "Iron": row["Iron"],
    "Water_Requirement": row["Water_Requirement"],
    "Radiation_Exposure": row["Radiation_Exposure"],
    "Magnesium": row["Magnesium"],

    # Leaf / Growth
    "Leaf_Initiation_Plastochron": row["Leaf_Initiation_Plastochron"],
    "Leaf_Elongation": row["Leaf_Elongation"],
    "Radiation": row["Radiation"],
    "Leaf_Expansion": row["Leaf_Expansion"],
    "Leaf_Length": row["Leaf_Length"],
    "Leaf_Width": row["Leaf_Width"],
    "Leaf_Count": row["Leaf_Count"],
    "Lower_Leaf_Length": row["Lower_Leaf_Length"],
    "Lower_Leaf_Width": row["Lower_Leaf_Width"],
    "Middle_Leaf_Length": row["Middle_Leaf_Length"],
    "Middle_Leaf_Width": row["Middle_Leaf_Width"],

    # Vegetation
    "Vegetation_Leaf_Area_Index": row["Vegetation_Leaf_Area_Index"],
    "Vegetation_Chlorophyll_Content": row["Vegetation_Chlorophyll_Content"],
    "Vegetation_Water_Stress_Index": row["Vegetation_Water_Stress_Index"],
    "Vegetation_Soil_Organic_Matter": row["Vegetation_Soil_Organic_Matter"],
    "Vegetation_Cloud_Cover": row["Vegetation_Cloud_Cover"],
    "Vegetation_Temperature_Range": row["Vegetation_Temperature_Range"],

    # Soil / Light / Flood / Humidity
    "Soil_Sensitivity_Highly": row["Soil_Sensitivity_Highly"],
    "Soil_Sensitivity_Moderate": row["Soil_Sensitivity_Moderate"],
    "Light_Intensity_Low": row["Light_Intensity_Low"],
    "Light_Intensity_Moderate": row["Light_Intensity_Moderate"],
    "Flood_Depth_Ideal": row["Flood_Depth_Ideal"],
    "Humidity_Moderate": row["Humidity_Moderate"],
    "Humidity_High": row["Humidity_High"],

    # Final Yield
    "Yield": row["Yield"]
}

            

            neo4j_connection.execute_query(query, params)

        print("Data successfully imported into Neo4j!")
    except Exception as e:
        print(f"Error creating relationships in Neo4j: {e}")
    
def export_tillering_relationships_from_neo4j(neo4j_connection, output_file):
    try:
        export_query = """
        MATCH (crop:Crop {name: 'Rice'})-[:HAS_DATE]->(date:Date)
OPTIONAL MATCH (date)-[:HAS_TIME_OF_DAY]->(timeOfDay:TimeOfDay)
OPTIONAL MATCH (timeOfDay)-[:HAS_CROP_STAGE]->(cropStage:CropStage)

// Weather Metrics
OPTIONAL MATCH (cropStage)-[:HAS_METRICS]->(LAT:LAT)
OPTIONAL MATCH (LAT)-[:HAS_METRICS]->(LON:LON)
OPTIONAL MATCH (LON)-[:HAS_METRICS]->(ts:TS)
OPTIONAL MATCH (ts)-[:HAS_METRICS]->(t2m:T2M)
OPTIONAL MATCH (t2m)-[:HAS_METRICS]->(rh2m:RH2M)
OPTIONAL MATCH (rh2m)-[:HAS_METRICS]->(ws2m:WS2M)
OPTIONAL MATCH (ws2m)-[:HAS_METRICS]->(t2mdew:T2MDEW)
OPTIONAL MATCH (t2mdew)-[:HAS_METRICS]->(gwettop:GWETTOP)
OPTIONAL MATCH (gwettop)-[:HAS_METRICS]->(cloudAmt:CLOUD_AMT)
OPTIONAL MATCH (cloudAmt)-[:HAS_METRICS]->(prectotcorr:PRECTOTCORR)

// Crop Information
OPTIONAL MATCH (cropStage)-[:HAS_VARIETY]->(variety:RiceVariety)
OPTIONAL MATCH (variety)-[:HAS_SEASON]->(season:Season)
OPTIONAL MATCH (season)-[:HAS_DURATION]->(duration:Duration)
OPTIONAL MATCH (duration)-[:HAS_DAY_OF_TILLERING]->(dayOfTillering:DayOfTillering)

// Nutrient / Soil / Water Parameters
OPTIONAL MATCH (dayOfTillering)-[:HAS_PARAMETERS]->(boron:Boron)
OPTIONAL MATCH (boron)-[:HAS_PARAMETERS]->(soil_Ph:Soil_Ph)
OPTIONAL MATCH (soil_Ph)-[:HAS_PARAMETERS]->(molybdenum:Molybdenum)
OPTIONAL MATCH (molybdenum)-[:HAS_PARAMETERS]->(chlorine:Chlorine)
OPTIONAL MATCH (chlorine)-[:HAS_PARAMETERS]->(nickel:Nickel)
OPTIONAL MATCH (nickel)-[:HAS_PARAMETERS]->(Sulphur:Sulphur)
OPTIONAL MATCH (Sulphur)-[:HAS_PARAMETERS]->(phosphorus:Phosphorus)
OPTIONAL MATCH (phosphorus)-[:HAS_PARAMETERS]->(potassium:Potassium)
OPTIONAL MATCH (potassium)-[:HAS_PARAMETERS]->(sodium:Sodium)
OPTIONAL MATCH (sodium)-[:HAS_PARAMETERS]->(nitrogen:Nitrogen)
OPTIONAL MATCH (nitrogen)-[:HAS_PARAMETERS]->(calcium:Calcium)
OPTIONAL MATCH (calcium)-[:HAS_PARAMETERS]->(zinc:Zinc)
OPTIONAL MATCH (zinc)-[:HAS_PARAMETERS]->(copper:Copper)
OPTIONAL MATCH (copper)-[:HAS_PARAMETERS]->(manganese:Manganese)
OPTIONAL MATCH (manganese)-[:HAS_PARAMETERS]->(iron:Iron)
OPTIONAL MATCH (iron)-[:HAS_PARAMETERS]->(Water_Requirement:Water_Requirement)
OPTIONAL MATCH (Water_Requirement)-[:HAS_PARAMETERS]->(Radiation_Exposure:Radiation_Exposure)
OPTIONAL MATCH (Radiation_Exposure)-[:HAS_PARAMETERS]->(Magnesium:Magnesium)

// Leaf / Growth Information
OPTIONAL MATCH (Magnesium)-[:HAS_LEAF_INFO]->(leafInitiation:LeafInitiationPlastochron)
OPTIONAL MATCH (leafInitiation)-[:HAS_LEAF_INFO]->(leafElongation:LeafElongation)
OPTIONAL MATCH (leafElongation)-[:HAS_RADIATION]->(radiation:Radiation)
OPTIONAL MATCH (radiation)-[:HAS_LEAF_INFO]->(leafExpansion:LeafExpansion)
OPTIONAL MATCH (leafExpansion)-[:HAS_DIMENSIONS]->(leafLength:LeafLength)
OPTIONAL MATCH (leafLength)-[:HAS_DIMENSIONS]->(leafWidth:LeafWidth)
OPTIONAL MATCH (leafWidth)-[:HAS_COUNT]->(leafCount:LeafCount)
OPTIONAL MATCH (leafCount)-[:HAS_DIMENSIONS]->(lowerLeafLength:LowerLeafLength)
OPTIONAL MATCH (lowerLeafLength)-[:HAS_DIMENSIONS]->(lowerLeafWidth:LowerLeafWidth)
OPTIONAL MATCH (lowerLeafWidth)-[:HAS_DIMENSIONS]->(middleLeafLength:MiddleLeafLength)
OPTIONAL MATCH (middleLeafLength)-[:HAS_DIMENSIONS]->(middleLeafWidth:MiddleLeafWidth)

// Vegetation Metrics
OPTIONAL MATCH (middleLeafWidth)-[:HAS_VEGETATION]->(vegLeafArea:VegetationLeafAreaIndex)
OPTIONAL MATCH (vegLeafArea)-[:HAS_CHLOROPHYLL]->(vegChlorophyll:VegetationChlorophyllContent)
OPTIONAL MATCH (vegChlorophyll)-[:HAS_WATER_STRESS]->(vegWaterStress:VegetationWaterStressIndex)
OPTIONAL MATCH (vegWaterStress)-[:HAS_SOIL]->(vegSoilOrganic:VegetationSoilOrganicMatter)
OPTIONAL MATCH (vegSoilOrganic)-[:HAS_CLOUD]->(vegCloudCover:VegetationCloudCover)
OPTIONAL MATCH (vegCloudCover)-[:HAS_TEMPERATURE]->(vegTempRange:VegetationTemperatureRange)

// Soil / Light / Flood / Humidity Metrics
OPTIONAL MATCH (vegTempRange)-[:HAS_SOIL_SENSITIVITY]->(soilSensitivityHigh:SoilSensitivityHighly)
OPTIONAL MATCH (soilSensitivityHigh)-[:HAS_SOIL_SENSITIVITY]->(soilSensitivityMod:SoilSensitivityModerate)
OPTIONAL MATCH (soilSensitivityMod)-[:HAS_LIGHT_INTENSITY]->(lightIntensityLow:LightIntensityLow)
OPTIONAL MATCH (lightIntensityLow)-[:HAS_LIGHT_INTENSITY]->(lightIntensityMod:LightIntensityModerate)
OPTIONAL MATCH (lightIntensityMod)-[:HAS_FLOOD_DEPTH]->(floodDepth:FloodDepthIdeal)
OPTIONAL MATCH (floodDepth)-[:HAS_HUMIDITY]->(humidityMod:HumidityModerate)
OPTIONAL MATCH (humidityMod)-[:HAS_HUMIDITY]->(humidityHigh:HumidityHigh)

// Yield
OPTIONAL MATCH (humidityHigh)-[:HAS_YIELD]->(yieldValue:Yield)

RETURN 
    date.value AS Date,
    timeOfDay.value AS TimeOfDay,
    cropStage.value AS Crop_Stage,
    LAT.value AS LAT,
    LON.value AS LON,
    ts.value AS TS,
    t2m.value AS T2M,
    rh2m.value AS RH2M,
    ws2m.value AS WS2M,
    t2mdew.value AS T2MDEW,
    gwettop.value AS GWETTOP,
    cloudAmt.value AS CLOUD_AMT,
    prectotcorr.value AS PRECTOTCORR,
    variety.value AS Rice_Crop_Variety,
    season.value AS Season,
    duration.value AS Duration,
    dayOfTillering.value AS Day_of_Tillering,
    boron.value AS Boron,
    soil_Ph.value AS Soil_Ph,
    molybdenum.value AS Molybdenum,
    chlorine.value AS Chlorine,
    nickel.value AS Nickel,
    Sulphur.value AS Sulphur,
    phosphorus.value AS Phosphorus,
    potassium.value AS Potassium,
    sodium.value AS Sodium,
    nitrogen.value AS Nitrogen,
    calcium.value AS Calcium,
    zinc.value AS Zinc,
    copper.value AS Copper,
    manganese.value AS Manganese,
    iron.value AS Iron,
    Water_Requirement.value AS Water_Requirement,
    Radiation_Exposure.value AS Radiation_Exposure,
    Magnesium.value AS Magnesium,
    leafInitiation.value AS Leaf_Initiation_Plastochron,
    leafElongation.value AS Leaf_Elongation,
    radiation.value AS Radiation,
    leafExpansion.value AS Leaf_Expansion,
    leafLength.value AS Leaf_Length,
    leafWidth.value AS Leaf_Width,
    leafCount.value AS Leaf_Count,
    lowerLeafLength.value AS Lower_Leaf_Length,
    lowerLeafWidth.value AS Lower_Leaf_Width,
    middleLeafLength.value AS Middle_Leaf_Length,
    middleLeafWidth.value AS Middle_Leaf_Width,
    vegLeafArea.value AS Vegetation_Leaf_Area_Index,
    vegChlorophyll.value AS Vegetation_Chlorophyll_Content,
    vegWaterStress.value AS Vegetation_Water_Stress_Index,
    vegSoilOrganic.value AS Vegetation_Soil_Organic_Matter,
    vegCloudCover.value AS Vegetation_Cloud_Cover,
    vegTempRange.value AS Vegetation_Temperature_Range,
    soilSensitivityHigh.value AS Soil_Sensitivity_Highly,
    soilSensitivityMod.value AS Soil_Sensitivity_Moderate,
    lightIntensityLow.value AS Light_Intensity_Low,
    lightIntensityMod.value AS Light_Intensity_Moderate,
    floodDepth.value AS Flood_Depth_Ideal,
    humidityMod.value AS Humidity_Moderate,
    humidityHigh.value AS Humidity_High,
    yieldValue.value AS Yield

        """

        results = neo4j_connection.execute_query(export_query)

        # Convert results to DataFrame and save as CSV
        df = pd.DataFrame(results)
        df.to_csv(output_file, index=False)
        print(f"Data successfully exported to {output_file}")
    except Exception as e:
        print(f"Error exporting data from Neo4j: {e}")

# Main execution
uri = "bolt://localhost:7687"
user = "neo4j"
password = "Naveen1@"
db_name = "Trilling"
output_file = "tillering_data_export111.csv"

# Establish connection
neo4j_conn = Neo4jConnection(uri, user, password, db_name)
create_tillering_relationships_in_neo4j(merged_df,neo4j_conn)
# Export data
export_tillering_relationships_from_neo4j(neo4j_conn, output_file)

# Close connection
neo4j_conn.close()