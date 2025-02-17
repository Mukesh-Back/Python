import random
import pandas as pd
from datetime import datetime, timedelta
from neo4j import GraphDatabase

class Neo4jConnection:
    def __init__(self, uri, user, password, db_name="leafdevelopment"):
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

def generate_leafdevelopment_stage_dataset():
    try:
        # Input date range from user
        start_date_input = input("Enter the start date (YYYY-MM-DD): ")
        end_date_input = input("Enter the end date (YYYY-MM-DD): ")

        start_date = datetime.strptime(start_date_input, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_input, "%Y-%m-%d")

        if start_date > end_date:
            raise ValueError("Start date must be earlier than end date.")

        # day_of_Leaf_development should cycle from 10 through 20
        LEAF_DEV_START = 10
        LEAF_DEV_END = 20
        
        # Define leaf parameters (split numeric and string/categorical)
        numeric_parameters = {
            "Boron": (0.5, 1.0),
            "Soil_ph": (0.5, 1.0),
            "Phosphorus": (15, 25),
            "Potassium": (100, 200),
            "Molybdenum": (100, 200),
            "Iron": (4.5, 10),
            "Sodium": (4.5, 10),
            "Chlorine": (4.5, 10),
            "Nickel": (4.5, 10),
            "Nitrogen": (200, 300),
            "Magnesium": (20, 60),
            "Calcium": (10, 20),
            "Sulphur": (10, 20),
            "Zinc": (1.0, 2.5),
            "Copper": (0.2, 0.5),
            "Manganese": (2.0, 5.0),
            "Soil_Sensitivity_Highly": (12, 14),
            "Soil_Sensitivity_Moderate": (14, 16),
            "Soil_Sensitivity_Insensitive": (16, 18),
            "Leaf_Production_High_Sensitivity_Short": (12, 14),
            "Leaf_Production_High_Sensitivity_Long": (14, 16),
            "Leaf_Production_Low_Sensitivity_Short": (16, 18),
            "Leaf_Production_Low_Sensitivity_Long": (16, 18),
            "Leaf_Initiation_Plastochron": (3, 5),
            "Leaf_Elongation": (1.0, 1.5),
            "Leaf_Expansion": (20, 50),
            "Plastochron_Interval": (3, 5),
            "Leaf_Length": (20, 50),
            "Leaf_Width": (1.0, 1.5),
            "Leaf_Count": (12, 18),
            "Leaf_Sheath_Length": (50, 70),
            "Lower_Leaf_Length": (15, 25),
            "Lower_Leaf_Width": (0.8, 1.2),
            "Middle_Leaf_Length": (30, 50),
            "Middle_Leaf_Width": (1.0, 1.5),
            "Flag_Leaf_Length": (20, 30),
            "Flag_Leaf_Width": (0.8, 1.0),
            "Vegetative_Duration_Short": (60, 90),
            "Vegetative_Duration_Long": (90, 130),
            "Lower_Leaves_Area": (30, 40),
            "Middle_Leaves_Area": (40, 60),
            "Flag_Leaf_Top_Area": (50, 70),
            "Senescence_Initiation": (50, 70),
            "Senescence_Middle_Phase": (70, 90),
            "Senescence_Full_Phase": (90, 120),
            "Light_Intensity_Low": (0, 2),
            "Light_Intensity_Moderate": (12, 14),
            "Light_Intensity_High": (14, 16),
            "Flood_Depth_Ideal": (5, 10),
            "Flood_Depth_Acceptable": (10, 15),
            "Flood_Depth_Excessive": (15, 20),
            "Water": (15, 20),
            "Humidity_Low": (0, 50),
            "Humidity_Moderate": (50, 70),
            "Temperature": (50, 70),
            "Humidity_High": (70, 90),
            "Humidity_Very_High": (90, 120),
            "Radiation": (90, 120)
        }

        categorical_parameters = {
            "Fungal_Diseases": [
                "Rice Blast (Magnaporthe oryzae)",
                "Sheath Blight (Rhizoctonia solani)",
                "Brown Spot (Bipolaris oryzae)",
                "Leaf Smut (Entyloma oryzae)"
            ],
            "Protective_Fungicides": ["Mancozeb", "Chlorothalonil", "Copper-based fungicides"],
            "Systemic_Fungicides": ["Azoxystrobin", "Propiconazole", "Tebuconazole", "Pyraclostrobin"],
            "Curative_Fungicides": ["Triadimefon", "Hexaconazole"],
            "Plant_Hormones": ["Auxins", "Gibberellins", "Cytokinins", "Brassinosteroids"]
        }

        crop_variety = random.choice(["Long", "Medium", "Short"])
        season = random.choice(["Rabi", "Kharif"])

        data = []
        current_date = start_date
        total_days = (end_date - start_date).days + 1
        times_of_day = ["morning", "afternoon", "evening"]

        accumulated_yield = 0
        days_elapsed = 0
        cycle_days = random.randint(120, 140)

        # Start day_of_Leaf_development at 10
        day_of_Leaf_development = LEAF_DEV_START

        for day in range(total_days):
            crop_stage = "Leaf_Development"

            for time_of_day in times_of_day:
                row = {
                    "Date": current_date.strftime("%d/%m/%Y"),
                    "TimeOfDay": time_of_day,
                    "Crop_Stage": crop_stage,
                    "Rice_Crop_Variety": crop_variety,
                    "Season": season,
                    "Duration": random.randint(90, 140),
                    "day_of_Leaf_development": day_of_Leaf_development,
                    "Yield": round(random.uniform(0, 100), 2),
                }

                # Generate numeric values
                for param, (min_val, max_val) in numeric_parameters.items():
                    if isinstance(min_val, float):
                        row[param] = round(random.uniform(min_val, max_val), 2)
                    else:
                        row[param] = random.randint(min_val, max_val)

                # Generate categorical values
                for param, choices in categorical_parameters.items():
                    row[param] = random.choice(choices)

                # Accumulate data
                data.append(row)

            # Move to the next day
            current_date += timedelta(days=1)
            days_elapsed += 1

            # Increment day_of_Leaf_development
            day_of_Leaf_development += 1
            # Once it exceeds 20, restart at 10
            if day_of_Leaf_development > LEAF_DEV_END:
                day_of_Leaf_development = LEAF_DEV_START

            # Check if we reached the random cycle_days
            if days_elapsed >= cycle_days:
                # Switch variety, season, yield trait for next cycle
                crop_variety = random.choice(["Long", "Medium", "Short"])
                season = "Kharif" if season == "Rabi" else "Rabi"
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

sowing_df = generate_leafdevelopment_stage_dataset()
power_df = pd.read_csv("power.csv")
merged_df = merge_power_data(sowing_df, power_df)
merged_df.to_csv("Leaf_Development_stage_dataset_last.csv", index=False)
print("Dataset generated and saved to 'Leaf_Development_stage_dataset_last.csv'.")

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
    "day_of_Leaf_development",

    # Leaf-nutrient parameters
    "Boron",
    "Soil_ph",
    "Phosphorus",
    "Potassium",
    "Molybdenum",
    "Iron", 
    "Sodium",
    "Chlorine",
    "Nickel",
    "Nitrogen",
    "Magnesium",
    "Calcium",
    "Sulphur", 
    "Zinc",
    "Copper",
    "Manganese",

    # Soil sensitivity
    "Soil_Sensitivity_Highly",
    "Soil_Sensitivity_Moderate",
    "Soil_Sensitivity_Insensitive",

    # Leaf production
    "Leaf_Production_High_Sensitivity_Short",
    "Leaf_Production_High_Sensitivity_Long", 
    "Leaf_Production_Low_Sensitivity_Short",
    "Leaf_Production_Low_Sensitivity_Long",

    # Leaf morphology and growth
    "Leaf_Initiation_Plastochron",
    "Leaf_Elongation",
    "Leaf_Expansion", 
    "Plastochron_Interval",
    "Leaf_Length",
    "Leaf_Width",
    "Leaf_Count", 
    "Leaf_Sheath_Length",
    "Lower_Leaf_Length",
    "Lower_Leaf_Width", 
    "Middle_Leaf_Length",
    "Middle_Leaf_Width",
    "Flag_Leaf_Length",
    "Flag_Leaf_Width", 
    "Vegetative_Duration_Short",
    "Vegetative_Duration_Long",

    # Fungal, fungicides, hormones
    "Fungal_Diseases",
    "Protective_Fungicides",
    "Systemic_Fungicides", 
    "Curative_Fungicides",
    "Plant_Hormones",

    # Leaf areas and senescence
    "Lower_Leaves_Area",
    "Middle_Leaves_Area",
    "Flag_Leaf_Top_Area", 
    "Senescence_Initiation",
    "Senescence_Middle_Phase",
    "Senescence_Full_Phase",

    # Light / Flood / Humidity / Water / Temperature / Radiation
    "Light_Intensity_Low",
    "Light_Intensity_Moderate",
    "Light_Intensity_High", 
    "Flood_Depth_Ideal",
    "Flood_Depth_Acceptable",
    "Flood_Depth_Excessive", 
    "Humidity_Low",
    "Humidity_Moderate",
    "Humidity_High",
    "Humidity_Very_High", 
    "Water",
    "Temperature",
    "Radiation",
    
    # Final yield
    "Yield"
]


merged_df = merged_df.reindex(columns=desired_columns)
#merged_df.to_csv("Leaf_Development_stage_dataset_reindex.csv", index=False)
def create_leaf_development_relationships_in_neo4j(merged_df, neo4j_connection):
    try:
        # Ensure the base "Rice" node exists
        neo4j_connection.execute_query("MERGE (rice:Crop {name: 'Rice'})")

        # Iterate through each row in the dataset
        for _, row in merged_df.iterrows():
            query = """
            MATCH (rice:Crop {name: 'Rice'})
            CREATE (date:Date {name: "Date", value: $Date})
            CREATE (timeOfDay:TimeOfDay {name: "TimeOfDay", value: $TimeOfDay})
            CREATE (lat:Latitude {name: "Latitude", value: $LAT})
            CREATE (lon:Longitude {name: "Longitude", value: $LON})
            CREATE (ts:TS {name: "TS", value: $TS})
            CREATE (t2m:T2M {name: "T2M", value: $T2M})
            CREATE (rh2m:RH2M {name: "RH2M", value: $RH2M})
            CREATE (ws2m:WS2M {name: "WS2M", value: $WS2M})
            CREATE (t2mdew:T2MDEW {name: "T2MDEW", value: $T2MDEW})
            CREATE (gwettop:GWETTOP {name: "GWETTOP", value: $GWETTOP})
            CREATE (cloud_amt:CLOUD_AMT {name: "CLOUD_AMT", value: $CLOUD_AMT})
            CREATE (prectotcorr:PRECTOTCORR {name: "PRECTOTCORR", value: $PRECTOTCORR})
            CREATE (crop_stage:CropStage {name: "CropStage", value: $Crop_Stage})
            CREATE (variety:RiceVariety {name: "RiceVariety", value: $Rice_Crop_Variety})
            CREATE (season:Season {name: "Season", value: $Season})
            CREATE (duration:Duration {name: "Duration", value: $Duration})
            CREATE (day_of_dev:DayOfLeafDevelopment {name: "DayOfLeafDevelopment", value: $day_of_Leaf_development})

            // --- Begin Nutrient Node Creation ---
            CREATE (boron:Boron {name: "Boron", value: $Boron})
            CREATE (soil_ph:Soil_ph {name: "Soil_ph", value: $Soil_ph})
            CREATE (phosphorus:Phosphorus {name: "Phosphorus", value: $Phosphorus})
            CREATE (potassium:Potassium {name: "Potassium", value: $Potassium})
            CREATE (molybdenum:Molybdenum {name: "Molybdenum", value: $Molybdenum})
            CREATE (iron:Iron {name: "Iron", value: $Iron})
            CREATE (sodium:Sodium {name: "Sodium", value: $Sodium})
            CREATE (chlorine:Chlorine {name: "Chlorine", value: $Chlorine})
            CREATE (nickel:Nickel {name: "Nickel", value: $Nickel})
            CREATE (nitrogen:Nitrogen {name: "Nitrogen", value: $Nitrogen})
            CREATE (magnesium:Magnesium {name: "Magnesium", value: $Magnesium})
            CREATE (calcium:Calcium {name: "Calcium", value: $Calcium})
            CREATE (sulphur:Sulphur {name: "Sulphur", value: $Sulphur})
            CREATE (zinc:Zinc {name: "Zinc", value: $Zinc})
            CREATE (copper:Copper {name: "Copper", value: $Copper})
            CREATE (manganese:Manganese {name: "Manganese", value: $Manganese})

            // --- Continue with Soil Sensitivity & Leaf Production ---
            CREATE (soil_sensitivity_highly:SoilSensitivityHighly {name: "SoilSensitivityHighly", value: $Soil_Sensitivity_Highly})
            CREATE (soil_sensitivity_moderate:SoilSensitivityModerate {name: "SoilSensitivityModerate", value: $Soil_Sensitivity_Moderate})
            CREATE (soil_sensitivity_insensitive:SoilSensitivityInsensitive {name: "SoilSensitivityInsensitive", value: $Soil_Sensitivity_Insensitive})
            CREATE (leaf_production_high_short:LeafProductionHighSensitivityShort {name: "LeafProductionHighSensitivityShort", value: $Leaf_Production_High_Sensitivity_Short})
            CREATE (leaf_production_high_long:LeafProductionHighSensitivityLong {name: "LeafProductionHighSensitivityLong", value: $Leaf_Production_High_Sensitivity_Long})
            CREATE (leaf_production_low_short:LeafProductionLowSensitivityShort {name: "LeafProductionLowSensitivityShort", value: $Leaf_Production_Low_Sensitivity_Short})
            CREATE (leaf_production_low_long:LeafProductionLowSensitivityLong {name: "LeafProductionLowSensitivityLong", value: $Leaf_Production_Low_Sensitivity_Long})
            CREATE (leaf_initiation_plastochron:LeafInitiationPlastochron {name: "LeafInitiationPlastochron", value: $Leaf_Initiation_Plastochron})
            CREATE (leaf_elongation:LeafElongation {name: "LeafElongation", value: $Leaf_Elongation})
            CREATE (leaf_expansion:LeafExpansion {name: "LeafExpansion", value: $Leaf_Expansion})
            CREATE (plastochron_interval:PlastochronInterval {name: "PlastochronInterval", value: $Plastochron_Interval})
            CREATE (leaf_length:LeafLength {name: "LeafLength", value: $Leaf_Length})
            CREATE (leaf_width:LeafWidth {name: "LeafWidth", value: $Leaf_Width})
            CREATE (leaf_count:LeafCount {name: "LeafCount", value: $Leaf_Count})
            CREATE (leaf_sheath_length:LeafSheathLength {name: "LeafSheathLength", value: $Leaf_Sheath_Length})
            CREATE (lower_leaf_length:LowerLeafLength {name: "LowerLeafLength", value: $Lower_Leaf_Length})
            CREATE (lower_leaf_width:LowerLeafWidth {name: "LowerLeafWidth", value: $Lower_Leaf_Width})
            CREATE (middle_leaf_length:MiddleLeafLength {name: "MiddleLeafLength", value: $Middle_Leaf_Length})
            CREATE (middle_leaf_width:MiddleLeafWidth {name: "MiddleLeafWidth", value: $Middle_Leaf_Width})
            CREATE (flag_leaf_length:FlagLeafLength {name: "FlagLeafLength", value: $Flag_Leaf_Length})
            CREATE (flag_leaf_width:FlagLeafWidth {name: "FlagLeafWidth", value: $Flag_Leaf_Width})
            CREATE (veg_duration_short:VegetativeDurationShort {name: "VegetativeDurationShort", value: $Vegetative_Duration_Short})
            CREATE (veg_duration_long:VegetativeDurationLong {name: "VegetativeDurationLong", value: $Vegetative_Duration_Long})

            // --- Fungal & Fungicides & Hormones ---
            CREATE (fungal_diseases:FungalDiseases {name: "FungalDiseases", value: $Fungal_Diseases})
            CREATE (protective_fungicides:ProtectiveFungicides {name: "ProtectiveFungicides", value: $Protective_Fungicides})
            CREATE (systemic_fungicides:SystemicFungicides {name: "SystemicFungicides", value: $Systemic_Fungicides})
            CREATE (curative_fungicides:CurativeFungicides {name: "CurativeFungicides", value: $Curative_Fungicides})
            CREATE (plant_hormones:PlantHormones {name: "PlantHormones", value: $Plant_Hormones})

            // --- Leaf Area & Senescence ---
            CREATE (lower_leaves_area:LowerLeavesArea {name: "LowerLeavesArea", value: $Lower_Leaves_Area})
            CREATE (middle_leaves_area:MiddleLeavesArea {name: "MiddleLeavesArea", value: $Middle_Leaves_Area})
            CREATE (flag_leaf_top_area:FlagLeafTopArea {name: "FlagLeafTopArea", value: $Flag_Leaf_Top_Area})
            CREATE (senescence_initiation:SenescenceInitiation {name: "SenescenceInitiation", value: $Senescence_Initiation})
            CREATE (senescence_middle:SenescenceMiddlePhase {name: "SenescenceMiddlePhase", value: $Senescence_Middle_Phase})
            CREATE (senescence_full:SenescenceFullPhase {name: "SenescenceFullPhase", value: $Senescence_Full_Phase})

            // --- Light / Flood / Humidity / Water / Temperature / Radiation ---
            CREATE (light_intensity_low:LightIntensityLow {name: "LightIntensityLow", value: $Light_Intensity_Low})
            CREATE (light_intensity_moderate:LightIntensityModerate {name: "LightIntensityModerate", value: $Light_Intensity_Moderate})
            CREATE (light_intensity_high:LightIntensityHigh {name: "LightIntensityHigh", value: $Light_Intensity_High})
            CREATE (flood_depth_ideal:FloodDepthIdeal {name: "FloodDepthIdeal", value: $Flood_Depth_Ideal})
            CREATE (flood_depth_acceptable:FloodDepthAcceptable {name: "FloodDepthAcceptable", value: $Flood_Depth_Acceptable})
            CREATE (flood_depth_excessive:FloodDepthExcessive {name: "FloodDepthExcessive", value: $Flood_Depth_Excessive})
            CREATE (humidity_low:HumidityLow {name: "HumidityLow", value: $Humidity_Low})
            CREATE (humidity_moderate:HumidityModerate {name: "HumidityModerate", value: $Humidity_Moderate})
            CREATE (humidity_high:HumidityHigh {name: "HumidityHigh", value: $Humidity_High})
            CREATE (humidity_very_high:HumidityVeryHigh {name: "HumidityVeryHigh", value: $Humidity_Very_High})

            CREATE (water:Water {name: "Water", value: $Water})
            CREATE (temperature:Temperature {name: "Temperature", value: $Temperature})
            CREATE (radiation:Radiation {name: "Radiation", value: $Radiation})

            CREATE (yield_value:Yield {name: 'Yield', value: $Yield})

            // --- Relationships (in a long chain) ---
            CREATE (rice)-[:HAS_DATE]->(date)
            CREATE (date)-[:HAS_TIME_OF_DAY]->(timeOfDay)
            CREATE (timeOfDay)-[:HAS_LATITUDE]->(lat)
            CREATE (lat)-[:HAS_LONGITUDE]->(lon)
            CREATE (lon)-[:HAS_TS]->(ts)
            CREATE (ts)-[:HAS_T2M]->(t2m)
            CREATE (t2m)-[:HAS_RH2M]->(rh2m)
            CREATE (rh2m)-[:HAS_WS2M]->(ws2m)
            CREATE (ws2m)-[:HAS_T2MDEW]->(t2mdew)
            CREATE (t2mdew)-[:HAS_GWETTOP]->(gwettop)
            CREATE (gwettop)-[:HAS_CLOUD_AMT]->(cloud_amt)
            CREATE (cloud_amt)-[:HAS_PRECTOTCORR]->(prectotcorr)
            CREATE (prectotcorr)-[:HAS_CROP_STAGE]->(crop_stage)
            CREATE (crop_stage)-[:HAS_VARIETY]->(variety)
            CREATE (variety)-[:HAS_SEASON]->(season)
            CREATE (season)-[:HAS_DURATION]->(duration)
            CREATE (duration)-[:HAS_DAY_OF_DEVELOPMENT]->(day_of_dev)

            // Chain for nutrients
            CREATE (day_of_dev)-[:HAS_BORON]->(boron)
            CREATE (boron)-[:HAS_SOIL_PH]->(soil_ph)
            CREATE (soil_ph)-[:HAS_PHOSPHORUS]->(phosphorus)
            CREATE (phosphorus)-[:HAS_POTASSIUM]->(potassium)
            CREATE (potassium)-[:HAS_MOLYBDENUM]->(molybdenum)
            CREATE (molybdenum)-[:HAS_IRON]->(iron)
            CREATE (iron)-[:HAS_SODIUM]->(sodium)
            CREATE (sodium)-[:HAS_CHLORINE]->(chlorine)
            CREATE (chlorine)-[:HAS_NICKEL]->(nickel)
            CREATE (nickel)-[:HAS_NITROGEN]->(nitrogen)
            CREATE (nitrogen)-[:HAS_MAGNESIUM]->(magnesium)
            CREATE (magnesium)-[:HAS_CALCIUM]->(calcium)
            CREATE (calcium)-[:HAS_SULPHUR]->(sulphur)
            CREATE (sulphur)-[:HAS_ZINC]->(zinc)
            CREATE (zinc)-[:HAS_COPPER]->(copper)
            CREATE (copper)-[:HAS_MANGANESE]->(manganese)

            // Soil Sensitivity & Leaf Production
            CREATE (manganese)-[:HAS_SOIL_SENSITIVITY_HIGHLY]->(soil_sensitivity_highly)
            CREATE (soil_sensitivity_highly)-[:HAS_SOIL_SENSITIVITY_MODERATE]->(soil_sensitivity_moderate)
            CREATE (soil_sensitivity_moderate)-[:HAS_SOIL_SENSITIVITY_INSENSITIVE]->(soil_sensitivity_insensitive)
            CREATE (soil_sensitivity_insensitive)-[:HAS_LEAF_PRODUCTION_HIGH_SHORT]->(leaf_production_high_short)
            CREATE (leaf_production_high_short)-[:HAS_LEAF_PRODUCTION_HIGH_LONG]->(leaf_production_high_long)
            CREATE (leaf_production_high_long)-[:HAS_LEAF_PRODUCTION_LOW_SHORT]->(leaf_production_low_short)
            CREATE (leaf_production_low_short)-[:HAS_LEAF_PRODUCTION_LOW_LONG]->(leaf_production_low_long)
            CREATE (leaf_production_low_long)-[:HAS_LEAF_INITIATION_PLASTOCHRON]->(leaf_initiation_plastochron)
            CREATE (leaf_initiation_plastochron)-[:HAS_LEAF_ELONGATION]->(leaf_elongation)
            CREATE (leaf_elongation)-[:HAS_LEAF_EXPANSION]->(leaf_expansion)
            CREATE (leaf_expansion)-[:HAS_PLASTOCHRON_INTERVAL]->(plastochron_interval)
            CREATE (plastochron_interval)-[:HAS_LEAF_LENGTH]->(leaf_length)
            CREATE (leaf_length)-[:HAS_LEAF_WIDTH]->(leaf_width)
            CREATE (leaf_width)-[:HAS_LEAF_COUNT]->(leaf_count)
            CREATE (leaf_count)-[:HAS_LEAF_SHEATH_LENGTH]->(leaf_sheath_length)
            CREATE (leaf_sheath_length)-[:HAS_LOWER_LEAF_LENGTH]->(lower_leaf_length)
            CREATE (lower_leaf_length)-[:HAS_LOWER_LEAF_WIDTH]->(lower_leaf_width)
            CREATE (lower_leaf_width)-[:HAS_MIDDLE_LEAF_LENGTH]->(middle_leaf_length)
            CREATE (middle_leaf_length)-[:HAS_MIDDLE_LEAF_WIDTH]->(middle_leaf_width)
            CREATE (middle_leaf_width)-[:HAS_FLAG_LEAF_LENGTH]->(flag_leaf_length)
            CREATE (flag_leaf_length)-[:HAS_FLAG_LEAF_WIDTH]->(flag_leaf_width)
            CREATE (flag_leaf_width)-[:HAS_VEG_DURATION_SHORT]->(veg_duration_short)
            CREATE (veg_duration_short)-[:HAS_VEG_DURATION_LONG]->(veg_duration_long)

            // Fungal & Fungicides
            CREATE (veg_duration_long)-[:HAS_FUNGAL_DISEASES]->(fungal_diseases)
            CREATE (fungal_diseases)-[:HAS_PROTECTIVE_FUNGICIDES]->(protective_fungicides)
            CREATE (protective_fungicides)-[:HAS_SYSTEMIC_FUNGICIDES]->(systemic_fungicides)
            CREATE (systemic_fungicides)-[:HAS_CURATIVE_FUNGICIDES]->(curative_fungicides)
            CREATE (curative_fungicides)-[:HAS_PLANT_HORMONES]->(plant_hormones)

            // Leaf area & senescence
            CREATE (plant_hormones)-[:HAS_LOWER_LEAVES_AREA]->(lower_leaves_area)
            CREATE (lower_leaves_area)-[:HAS_MIDDLE_LEAVES_AREA]->(middle_leaves_area)
            CREATE (middle_leaves_area)-[:HAS_FLAG_LEAF_TOP_AREA]->(flag_leaf_top_area)
            CREATE (flag_leaf_top_area)-[:HAS_SENESCENCE_INITIATION]->(senescence_initiation)
            CREATE (senescence_initiation)-[:HAS_SENESCENCE_MIDDLE]->(senescence_middle)
            CREATE (senescence_middle)-[:HAS_SENESCENCE_FULL]->(senescence_full)

            // Light / Flood / Humidity / Water / Temperature / Radiation
            CREATE (senescence_full)-[:HAS_LIGHT_INTENSITY_LOW]->(light_intensity_low)
            CREATE (light_intensity_low)-[:HAS_LIGHT_INTENSITY_MODERATE]->(light_intensity_moderate)
            CREATE (light_intensity_moderate)-[:HAS_LIGHT_INTENSITY_HIGH]->(light_intensity_high)
            CREATE (light_intensity_high)-[:HAS_FLOOD_DEPTH_IDEAL]->(flood_depth_ideal)
            CREATE (flood_depth_ideal)-[:HAS_FLOOD_DEPTH_ACCEPTABLE]->(flood_depth_acceptable)
            CREATE (flood_depth_acceptable)-[:HAS_FLOOD_DEPTH_EXCESSIVE]->(flood_depth_excessive)
            CREATE (flood_depth_excessive)-[:HAS_HUMIDITY_LOW]->(humidity_low)
            CREATE (humidity_low)-[:HAS_HUMIDITY_MODERATE]->(humidity_moderate)
            CREATE (humidity_moderate)-[:HAS_HUMIDITY_HIGH]->(humidity_high)
            CREATE (humidity_high)-[:HAS_HUMIDITY_VERY_HIGH]->(humidity_very_high)

            CREATE (humidity_very_high)-[:HAS_WATER]->(water)
            CREATE (water)-[:HAS_TEMPERATURE]->(temperature)
            CREATE (temperature)-[:HAS_RADIATION]->(radiation)

            // Final link to yield
            CREATE (radiation)-[:HAS_YIELD]->(yield_value)
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
                "Rice_Crop_Variety": row["Rice_Crop_Variety"],
                "Season": row["Season"],
                "Duration": row["Duration"],
                "day_of_Leaf_development": row["day_of_Leaf_development"],

                # Nutrients
                "Boron": row["Boron"],
                "Soil_ph": row["Soil_ph"],
                "Phosphorus": row["Phosphorus"],
                "Potassium": row["Potassium"],
                "Molybdenum": row["Molybdenum"],
                "Iron": row["Iron"],
                "Sodium": row["Sodium"],
                "Chlorine": row["Chlorine"],
                "Nickel": row["Nickel"],
                "Nitrogen": row["Nitrogen"],
                "Magnesium": row["Magnesium"],
                "Calcium": row["Calcium"],
                "Sulphur": row["Sulphur"],
                "Zinc": row["Zinc"],
                "Copper": row["Copper"],
                "Manganese": row["Manganese"],

                # Soil Sensitivity
                "Soil_Sensitivity_Highly": row["Soil_Sensitivity_Highly"],
                "Soil_Sensitivity_Moderate": row["Soil_Sensitivity_Moderate"],
                "Soil_Sensitivity_Insensitive": row["Soil_Sensitivity_Insensitive"],

                # Leaf Production
                "Leaf_Production_High_Sensitivity_Short": row["Leaf_Production_High_Sensitivity_Short"],
                "Leaf_Production_High_Sensitivity_Long": row["Leaf_Production_High_Sensitivity_Long"],
                "Leaf_Production_Low_Sensitivity_Short": row["Leaf_Production_Low_Sensitivity_Short"],
                "Leaf_Production_Low_Sensitivity_Long": row["Leaf_Production_Low_Sensitivity_Long"],
                "Leaf_Initiation_Plastochron": row["Leaf_Initiation_Plastochron"],
                "Leaf_Elongation": row["Leaf_Elongation"],
                "Leaf_Expansion": row["Leaf_Expansion"],
                "Plastochron_Interval": row["Plastochron_Interval"],
                "Leaf_Length": row["Leaf_Length"],
                "Leaf_Width": row["Leaf_Width"],
                "Leaf_Count": row["Leaf_Count"],
                "Leaf_Sheath_Length": row["Leaf_Sheath_Length"],
                "Lower_Leaf_Length": row["Lower_Leaf_Length"],
                "Lower_Leaf_Width": row["Lower_Leaf_Width"],
                "Middle_Leaf_Length": row["Middle_Leaf_Length"],
                "Middle_Leaf_Width": row["Middle_Leaf_Width"],
                "Flag_Leaf_Length": row["Flag_Leaf_Length"],
                "Flag_Leaf_Width": row["Flag_Leaf_Width"],
                "Vegetative_Duration_Short": row["Vegetative_Duration_Short"],
                "Vegetative_Duration_Long": row["Vegetative_Duration_Long"],

                # Fungal & Fungicides & Hormones
                "Fungal_Diseases": row["Fungal_Diseases"],
                "Protective_Fungicides": row["Protective_Fungicides"],
                "Systemic_Fungicides": row["Systemic_Fungicides"],
                "Curative_Fungicides": row["Curative_Fungicides"],
                "Plant_Hormones": row["Plant_Hormones"],

                # Leaf area & senescence
                "Lower_Leaves_Area": row["Lower_Leaves_Area"],
                "Middle_Leaves_Area": row["Middle_Leaves_Area"],
                "Flag_Leaf_Top_Area": row["Flag_Leaf_Top_Area"],
                "Senescence_Initiation": row["Senescence_Initiation"],
                "Senescence_Middle_Phase": row["Senescence_Middle_Phase"],
                "Senescence_Full_Phase": row["Senescence_Full_Phase"],

                # Light / Flood / Humidity / Water / Temperature / Radiation
                "Light_Intensity_Low": row["Light_Intensity_Low"],
                "Light_Intensity_Moderate": row["Light_Intensity_Moderate"],
                "Light_Intensity_High": row["Light_Intensity_High"],
                "Flood_Depth_Ideal": row["Flood_Depth_Ideal"],
                "Flood_Depth_Acceptable": row["Flood_Depth_Acceptable"],
                "Flood_Depth_Excessive": row["Flood_Depth_Excessive"],
                "Humidity_Low": row["Humidity_Low"],
                "Humidity_Moderate": row["Humidity_Moderate"],
                "Humidity_High": row["Humidity_High"],
                "Humidity_Very_High": row["Humidity_Very_High"],

                "Water": row["Water"],
                "Temperature": row["Temperature"],
                "Radiation": row["Radiation"],

                # Yield
                "Yield": row["Yield"]
            }

            # Execute the query for the current row
            neo4j_connection.execute_query(query, params)

        print("Data successfully imported into Neo4j!")
    except Exception as e:
        print(f"Error creating relationships in Neo4j: {e}")

def export_leaf_development_relationships_from_neo4j(neo4j_connection, output_file):
    try:
        export_query = """
        MATCH (date:Date)
        OPTIONAL MATCH (date)-[:HAS_TIME_OF_DAY]->(timeOfDay:TimeOfDay)
        OPTIONAL MATCH (timeOfDay)-[:HAS_LATITUDE]->(lat:Latitude)
        OPTIONAL MATCH (lat)-[:HAS_LONGITUDE]->(lon:Longitude)
        OPTIONAL MATCH (lon)-[:HAS_TS]->(ts:TS)
        OPTIONAL MATCH (ts)-[:HAS_T2M]->(t2m:T2M)
        OPTIONAL MATCH (t2m)-[:HAS_RH2M]->(rh2m:RH2M)
        OPTIONAL MATCH (rh2m)-[:HAS_WS2M]->(ws2m:WS2M)
        OPTIONAL MATCH (ws2m)-[:HAS_T2MDEW]->(t2mdew:T2MDEW)
        OPTIONAL MATCH (t2mdew)-[:HAS_GWETTOP]->(gwettop:GWETTOP)
        OPTIONAL MATCH (gwettop)-[:HAS_CLOUD_AMT]->(cloud_amt:CLOUD_AMT)
        OPTIONAL MATCH (cloud_amt)-[:HAS_PRECTOTCORR]->(prectotcorr:PRECTOTCORR)
        OPTIONAL MATCH (prectotcorr)-[:HAS_CROP_STAGE]->(crop_stage:CropStage)
        OPTIONAL MATCH (crop_stage)-[:HAS_VARIETY]->(variety:RiceVariety)
        OPTIONAL MATCH (variety)-[:HAS_SEASON]->(season:Season)
        OPTIONAL MATCH (season)-[:HAS_DURATION]->(duration:Duration)
        OPTIONAL MATCH (duration)-[:HAS_DAY_OF_DEVELOPMENT]->(day_of_dev:DayOfLeafDevelopment)

        // Nutrients
        OPTIONAL MATCH (day_of_dev)-[:HAS_BORON]->(boron:Boron)
        OPTIONAL MATCH (boron)-[:HAS_SOIL_PH]->(soil_ph:Soil_ph)
        OPTIONAL MATCH (soil_ph)-[:HAS_PHOSPHORUS]->(phosphorus:Phosphorus)
        OPTIONAL MATCH (phosphorus)-[:HAS_POTASSIUM]->(potassium:Potassium)
        OPTIONAL MATCH (potassium)-[:HAS_MOLYBDENUM]->(molybdenum:Molybdenum)
        OPTIONAL MATCH (molybdenum)-[:HAS_IRON]->(iron:Iron)
        OPTIONAL MATCH (iron)-[:HAS_SODIUM]->(sodium:Sodium)
        OPTIONAL MATCH (sodium)-[:HAS_CHLORINE]->(chlorine:Chlorine)
        OPTIONAL MATCH (chlorine)-[:HAS_NICKEL]->(nickel:Nickel)
        OPTIONAL MATCH (nickel)-[:HAS_NITROGEN]->(nitrogen:Nitrogen)
        OPTIONAL MATCH (nitrogen)-[:HAS_MAGNESIUM]->(magnesium:Magnesium)
        OPTIONAL MATCH (magnesium)-[:HAS_CALCIUM]->(calcium:Calcium)
        OPTIONAL MATCH (calcium)-[:HAS_SULPHUR]->(sulphur:Sulphur)
        OPTIONAL MATCH (sulphur)-[:HAS_ZINC]->(zinc:Zinc)
        OPTIONAL MATCH (zinc)-[:HAS_COPPER]->(copper:Copper)
        OPTIONAL MATCH (copper)-[:HAS_MANGANESE]->(manganese:Manganese)

        // Soil Sensitivity & Leaf Production
        OPTIONAL MATCH (manganese)-[:HAS_SOIL_SENSITIVITY_HIGHLY]->(soil_sensitivity_highly:SoilSensitivityHighly)
        OPTIONAL MATCH (soil_sensitivity_highly)-[:HAS_SOIL_SENSITIVITY_MODERATE]->(soil_sensitivity_moderate:SoilSensitivityModerate)
        OPTIONAL MATCH (soil_sensitivity_moderate)-[:HAS_SOIL_SENSITIVITY_INSENSITIVE]->(soil_sensitivity_insensitive:SoilSensitivityInsensitive)
        OPTIONAL MATCH (soil_sensitivity_insensitive)-[:HAS_LEAF_PRODUCTION_HIGH_SHORT]->(leaf_production_high_short:LeafProductionHighSensitivityShort)
        OPTIONAL MATCH (leaf_production_high_short)-[:HAS_LEAF_PRODUCTION_HIGH_LONG]->(leaf_production_high_long:LeafProductionHighSensitivityLong)
        OPTIONAL MATCH (leaf_production_high_long)-[:HAS_LEAF_PRODUCTION_LOW_SHORT]->(leaf_production_low_short:LeafProductionLowSensitivityShort)
        OPTIONAL MATCH (leaf_production_low_short)-[:HAS_LEAF_PRODUCTION_LOW_LONG]->(leaf_production_low_long:LeafProductionLowSensitivityLong)
        OPTIONAL MATCH (leaf_production_low_long)-[:HAS_LEAF_INITIATION_PLASTOCHRON]->(leaf_initiation_plastochron:LeafInitiationPlastochron)
        OPTIONAL MATCH (leaf_initiation_plastochron)-[:HAS_LEAF_ELONGATION]->(leaf_elongation:LeafElongation)
        OPTIONAL MATCH (leaf_elongation)-[:HAS_LEAF_EXPANSION]->(leaf_expansion:LeafExpansion)
        OPTIONAL MATCH (leaf_expansion)-[:HAS_PLASTOCHRON_INTERVAL]->(plastochron_interval:PlastochronInterval)
        OPTIONAL MATCH (plastochron_interval)-[:HAS_LEAF_LENGTH]->(leaf_length:LeafLength)
        OPTIONAL MATCH (leaf_length)-[:HAS_LEAF_WIDTH]->(leaf_width:LeafWidth)
        OPTIONAL MATCH (leaf_width)-[:HAS_LEAF_COUNT]->(leaf_count:LeafCount)
        OPTIONAL MATCH (leaf_count)-[:HAS_LEAF_SHEATH_LENGTH]->(leaf_sheath_length:LeafSheathLength)
        OPTIONAL MATCH (leaf_sheath_length)-[:HAS_LOWER_LEAF_LENGTH]->(lower_leaf_length:LowerLeafLength)
        OPTIONAL MATCH (lower_leaf_length)-[:HAS_LOWER_LEAF_WIDTH]->(lower_leaf_width:LowerLeafWidth)
        OPTIONAL MATCH (lower_leaf_width)-[:HAS_MIDDLE_LEAF_LENGTH]->(middle_leaf_length:MiddleLeafLength)
        OPTIONAL MATCH (middle_leaf_length)-[:HAS_MIDDLE_LEAF_WIDTH]->(middle_leaf_width:MiddleLeafWidth)
        OPTIONAL MATCH (middle_leaf_width)-[:HAS_FLAG_LEAF_LENGTH]->(flag_leaf_length:FlagLeafLength)
        OPTIONAL MATCH (flag_leaf_length)-[:HAS_FLAG_LEAF_WIDTH]->(flag_leaf_width:FlagLeafWidth)
        OPTIONAL MATCH (flag_leaf_width)-[:HAS_VEG_DURATION_SHORT]->(veg_duration_short:VegetativeDurationShort)
        OPTIONAL MATCH (veg_duration_short)-[:HAS_VEG_DURATION_LONG]->(veg_duration_long:VegetativeDurationLong)

        // Fungal & Fungicides
        OPTIONAL MATCH (veg_duration_long)-[:HAS_FUNGAL_DISEASES]->(fungal_diseases:FungalDiseases)
        OPTIONAL MATCH (fungal_diseases)-[:HAS_PROTECTIVE_FUNGICIDES]->(protective_fungicides:ProtectiveFungicides)
        OPTIONAL MATCH (protective_fungicides)-[:HAS_SYSTEMIC_FUNGICIDES]->(systemic_fungicides:SystemicFungicides)
        OPTIONAL MATCH (systemic_fungicides)-[:HAS_CURATIVE_FUNGICIDES]->(curative_fungicides:CurativeFungicides)
        OPTIONAL MATCH (curative_fungicides)-[:HAS_PLANT_HORMONES]->(plant_hormones:PlantHormones)

        // Leaf area & senescence
        OPTIONAL MATCH (plant_hormones)-[:HAS_LOWER_LEAVES_AREA]->(lower_leaves_area:LowerLeavesArea)
        OPTIONAL MATCH (lower_leaves_area)-[:HAS_MIDDLE_LEAVES_AREA]->(middle_leaves_area:MiddleLeavesArea)
        OPTIONAL MATCH (middle_leaves_area)-[:HAS_FLAG_LEAF_TOP_AREA]->(flag_leaf_top_area:FlagLeafTopArea)
        OPTIONAL MATCH (flag_leaf_top_area)-[:HAS_SENESCENCE_INITIATION]->(senescence_initiation:SenescenceInitiation)
        OPTIONAL MATCH (senescence_initiation)-[:HAS_SENESCENCE_MIDDLE]->(senescence_middle:SenescenceMiddlePhase)
        OPTIONAL MATCH (senescence_middle)-[:HAS_SENESCENCE_FULL]->(senescence_full:SenescenceFullPhase)

        // Light / Flood / Humidity / Water / Temperature / Radiation
        OPTIONAL MATCH (senescence_full)-[:HAS_LIGHT_INTENSITY_LOW]->(light_intensity_low:LightIntensityLow)
        OPTIONAL MATCH (light_intensity_low)-[:HAS_LIGHT_INTENSITY_MODERATE]->(light_intensity_moderate:LightIntensityModerate)
        OPTIONAL MATCH (light_intensity_moderate)-[:HAS_LIGHT_INTENSITY_HIGH]->(light_intensity_high:LightIntensityHigh)
        OPTIONAL MATCH (light_intensity_high)-[:HAS_FLOOD_DEPTH_IDEAL]->(flood_depth_ideal:FloodDepthIdeal)
        OPTIONAL MATCH (flood_depth_ideal)-[:HAS_FLOOD_DEPTH_ACCEPTABLE]->(flood_depth_acceptable:FloodDepthAcceptable)
        OPTIONAL MATCH (flood_depth_acceptable)-[:HAS_FLOOD_DEPTH_EXCESSIVE]->(flood_depth_excessive:FloodDepthExcessive)
        OPTIONAL MATCH (flood_depth_excessive)-[:HAS_HUMIDITY_LOW]->(humidity_low:HumidityLow)
        OPTIONAL MATCH (humidity_low)-[:HAS_HUMIDITY_MODERATE]->(humidity_moderate:HumidityModerate)
        OPTIONAL MATCH (humidity_moderate)-[:HAS_HUMIDITY_HIGH]->(humidity_high:HumidityHigh)
        OPTIONAL MATCH (humidity_high)-[:HAS_HUMIDITY_VERY_HIGH]->(humidity_very_high:HumidityVeryHigh)

        OPTIONAL MATCH (humidity_very_high)-[:HAS_WATER]->(water:Water)
        OPTIONAL MATCH (water)-[:HAS_TEMPERATURE]->(temperature:Temperature)
        OPTIONAL MATCH (temperature)-[:HAS_RADIATION]->(radiation:Radiation)

        // Final yield
        OPTIONAL MATCH (radiation)-[:HAS_YIELD]->(yield_value:Yield)

        RETURN
            date.value AS Date,
            timeOfDay.value AS TimeOfDay,
            lat.value AS LAT,
            lon.value AS LON,
            ts.value AS TS,
            t2m.value AS T2M,
            rh2m.value AS RH2M,
            ws2m.value AS WS2M,
            t2mdew.value AS T2MDEW,
            gwettop.value AS GWETTOP,
            cloud_amt.value AS CLOUD_AMT,
            prectotcorr.value AS PRECTOTCORR,
            crop_stage.value AS Crop_Stage,
            variety.value AS Rice_Crop_Variety,
            season.value AS Season,
            duration.value AS Duration,
            day_of_dev.value AS Day_of_Leaf_Development,

            boron.value AS Boron,
            soil_ph.value AS Soil_ph,
            phosphorus.value AS Phosphorus,
            potassium.value AS Potassium,
            molybdenum.value AS Molybdenum,
            iron.value AS Iron,
            sodium.value AS Sodium,
            chlorine.value AS Chlorine,
            nickel.value AS Nickel,
            nitrogen.value AS Nitrogen,
            magnesium.value AS Magnesium,
            calcium.value AS Calcium,
            sulphur.value AS Sulphur,
            zinc.value AS Zinc,
            copper.value AS Copper,
            manganese.value AS Manganese,

            soil_sensitivity_highly.value AS Soil_Sensitivity_Highly,
            soil_sensitivity_moderate.value AS Soil_Sensitivity_Moderate,
            soil_sensitivity_insensitive.value AS Soil_Sensitivity_Insensitive,
            leaf_production_high_short.value AS Leaf_Production_High_Sensitivity_Short,
            leaf_production_high_long.value AS Leaf_Production_High_Sensitivity_Long,
            leaf_production_low_short.value AS Leaf_Production_Low_Sensitivity_Short,
            leaf_production_low_long.value AS Leaf_Production_Low_Sensitivity_Long,
            leaf_initiation_plastochron.value AS Leaf_Initiation_Plastochron,
            leaf_elongation.value AS Leaf_Elongation,
            leaf_expansion.value AS Leaf_Expansion,
            plastochron_interval.value AS Plastochron_Interval,
            leaf_length.value AS Leaf_Length,
            leaf_width.value AS Leaf_Width,
            leaf_count.value AS Leaf_Count,
            leaf_sheath_length.value AS Leaf_Sheath_Length,
            lower_leaf_length.value AS Lower_Leaf_Length,
            lower_leaf_width.value AS Lower_Leaf_Width,
            middle_leaf_length.value AS Middle_Leaf_Length,
            middle_leaf_width.value AS Middle_Leaf_Width,
            flag_leaf_length.value AS Flag_Leaf_Length,
            flag_leaf_width.value AS Flag_Leaf_Width,
            veg_duration_short.value AS Vegetative_Duration_Short,
            veg_duration_long.value AS Vegetative_Duration_Long,

            fungal_diseases.value AS Fungal_Diseases,
            protective_fungicides.value AS Protective_Fungicides,
            systemic_fungicides.value AS Systemic_Fungicides,
            curative_fungicides.value AS Curative_Fungicides,
            plant_hormones.value AS Plant_Hormones,

            lower_leaves_area.value AS Lower_Leaves_Area,
            middle_leaves_area.value AS Middle_Leaves_Area,
            flag_leaf_top_area.value AS Flag_Leaf_Top_Area,
            senescence_initiation.value AS Senescence_Initiation,
            senescence_middle.value AS Senescence_Middle_Phase,
            senescence_full.value AS Senescence_Full_Phase,

            light_intensity_low.value AS Light_Intensity_Low,
            light_intensity_moderate.value AS Light_Intensity_Moderate,
            light_intensity_high.value AS Light_Intensity_High,

            flood_depth_ideal.value AS Flood_Depth_Ideal,
            flood_depth_acceptable.value AS Flood_Depth_Acceptable,
            flood_depth_excessive.value AS Flood_Depth_Excessive,

            humidity_low.value AS Humidity_Low,
            humidity_moderate.value AS Humidity_Moderate,
            humidity_high.value AS Humidity_High,
            humidity_very_high.value AS Humidity_Very_High,

            water.value AS Water,
            temperature.value AS Temperature,
            radiation.value AS Radiation,

            yield_value.value AS Yield
        """

        # Execute the query and fetch the results
        results = neo4j_connection.execute_query(export_query)

        # Convert results to a DataFrame
        df = pd.DataFrame(results)

        # Save DataFrame to a CSV file
        df.to_csv(output_file, index=False)
        print(f"Data successfully exported to {output_file}")
    except Exception as e:
        print(f"Error exporting data from Neo4j: {e}")

uri = "bolt://192.168.0.36:7687"
user = "neo4j"
password = "Naveen1@"
db_name="leafdevelopment"
neo4j_conn = Neo4jConnection(uri, user, password,db_name="leafdevelopment")
create_leaf_development_relationships_in_neo4j(merged_df,neo4j_conn)
    # Export Tillering Stage Data
export_leaf_development_relationships_from_neo4j(neo4j_conn, "exported_leafdevelopment_data.csv")

neo4j_conn.close()

