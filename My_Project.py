import pandas as pd
from neo4j import GraphDatabase
import pickle
import matplotlib.pyplot as plt
from sklearn import preprocessing

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



def export_germination_relationships_from_neo4j(neo4j_conn):
    try:
        # Define the query to export data in the same order as it was imported
        export_query = """
        MATCH (crop:Crop {name: 'Rice'})-[:HAS_DATE]->(date:Date)
        OPTIONAL MATCH (date)-[:HAS_TIME_OF_DAY]->(timeOfDay:TimeOfDay)
        OPTIONAL MATCH (timeOfDay)-[:HAS_LAT]->(lat:LAT)
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
        OPTIONAL MATCH (cropStage)-[:HAS_VARIETY]->(variety:CropVarietyType)
        OPTIONAL MATCH (variety)-[:HAS_SEASON]->(season:Season)
        OPTIONAL MATCH (season)-[:HAS_DURATION]->(duration:Duration)
        OPTIONAL MATCH (duration)-[:HAS_DAY_OF_CROP]->(dayOfCrop:DayOfCrop)
        OPTIONAL MATCH (dayOfCrop)-[:HAS_SOIL_MOISTURE_CONTENT]->(soilMoistureContent:SoilMoistureContent)
        OPTIONAL MATCH (soilMoistureContent)-[:HAS_SOIL_TEMPERATURE]->(soilTemperature:SoilTemperature)
        OPTIONAL MATCH (soilTemperature)-[:HAS_SOIL_PH]->(soilPH:SoilPH)
        OPTIONAL MATCH (soilPH)-[:HAS_DAILY_WATERING_LEVEL]->(dailyWateringLevel:DailyWateringLevel)
        OPTIONAL MATCH (dailyWateringLevel)-[:HAS_WATER_RETENTION_CAPACITY]->(waterRetentionCapacity:WaterRetentionCapacity)
        OPTIONAL MATCH (waterRetentionCapacity)-[:HAS_AMBIENT_TEMPERATURE]->(ambientTemperature:AmbientTemperature)
        OPTIONAL MATCH (ambientTemperature)-[:HAS_HUMIDITY]->(humidity:Humidity)
        OPTIONAL MATCH (humidity)-[:HAS_SUNLIGHT_EXPOSURE]->(sunlightExposure:SunlightExposure)
        OPTIONAL MATCH (sunlightExposure)-[:HAS_RAINFALL]->(rainfall:Rainfall)
        OPTIONAL MATCH (rainfall)-[:HAS_NITROGEN]->(nitrogen:Nitrogen)
        OPTIONAL MATCH (nitrogen)-[:HAS_PHOSPHORUS]->(phosphorus:Phosphorus)
        OPTIONAL MATCH (phosphorus)-[:HAS_POTASSIUM]->(potassium:Potassium)
        OPTIONAL MATCH (potassium)-[:HAS_ZINC]->(zinc:Zinc)
        OPTIONAL MATCH (zinc)-[:HAS_IRON]->(iron:Iron)
        OPTIONAL MATCH (iron)-[:HAS_MAGNESIUM]->(magnesium:Magnesium)
        OPTIONAL MATCH (magnesium)-[:HAS_SODIUM]->(sodium:Sodium)
        OPTIONAL MATCH (sodium)-[:HAS_SEED_VIGOR_INDEX]->(seedVigorIndex:SeedVigorIndex)
        OPTIONAL MATCH (seedVigorIndex)-[:HAS_GERMINATION_RATE]->(germinationRate:GerminationRate)
        OPTIONAL MATCH (germinationRate)-[:HAS_DISEASE_RESISTANCE]->(diseaseResistance:DiseaseResistance)
        OPTIONAL MATCH (diseaseResistance)-[:HAS_PEST_RESISTANCE]->(pestResistance:PestResistance)
        OPTIONAL MATCH (pestResistance)-[:HAS_PLANTING_DEPTH]->(plantingDepth:PlantingDepth)
        OPTIONAL MATCH (plantingDepth)-[:HAS_SEED_SPACING]->(seedSpacing:SeedSpacing)
        OPTIONAL MATCH (seedSpacing)-[:HAS_VEGETATION_INDEX]->(vegetationIndex:VegetationIndex)
        OPTIONAL MATCH (vegetationIndex)-[:HAS_SEED_VIABILITY]->(seedViability:SeedViability)
        OPTIONAL MATCH (seedViability)-[:HAS_SEED_MOISTURE_CONTENT]->(seedMoistureContent:SeedMoistureContent)
        OPTIONAL MATCH (seedMoistureContent)-[:HAS_HEAT_STRESS_INDEX]->(heatStressIndex:HeatStressIndex)
        OPTIONAL MATCH (heatStressIndex)-[:HAS_WATER_STRESS_INDEX]->(waterStressIndex:WaterStressIndex)
        OPTIONAL MATCH (waterStressIndex)-[:HAS_SALINITY_STRESS_INDEX]->(salinityStressIndex:SalinityStressIndex)
        OPTIONAL MATCH (salinityStressIndex)-[:HAS_ROOT_GROWTH_RATE]->(rootGrowthRate:RootGrowthRate)
        OPTIONAL MATCH (rootGrowthRate)-[:HAS_SHOOT_EMERGENCE_TIME]->(shootEmergenceTime:ShootEmergenceTime)
        OPTIONAL MATCH (shootEmergenceTime)-[:HAS_SOIL_RESPIRATION_RATE]->(soilRespirationRate:SoilRespirationRate)
        OPTIONAL MATCH (soilRespirationRate)-[:HAS_ORGANIC_MATTER_CONTENT]->(organicMatterContent:OrganicMatterContent)
        OPTIONAL MATCH (organicMatterContent)-[:HAS_CATION_EXCHANGE_CAPACITY]->(cationExchangeCapacity:CationExchangeCapacity)
        OPTIONAL MATCH (cationExchangeCapacity)-[:HAS_ELECTRICAL_CONDUCTIVITY]->(electricalConductivity:ElectricalConductivity)
        OPTIONAL MATCH (electricalConductivity)-[:HAS_WIND_SPEED]->(windSpeed:WindSpeed)
        OPTIONAL MATCH (windSpeed)-[:HAS_CLOUD_COVER]->(cloudCover:CloudCover)
        OPTIONAL MATCH (cloudCover)-[:HAS_RADIATION_EXPOSURE]->(radiationExposure:RadiationExposure)
        OPTIONAL MATCH (radiationExposure)-[:HAS_INITIAL_FERTILIZER_APPLICATION]->(initialFertilizerApplication:InitialFertilizerApplication)
        OPTIONAL MATCH (initialFertilizerApplication)-[:HAS_YIELD]->(yieldNode:Yield)
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
            dailyWateringLevel.value AS Daily_Watering_Level, 
            waterRetentionCapacity.value AS Water_Retention_Capacity, 
            ambientTemperature.value AS Ambient_Temperature, 
            humidity.value AS Humidity, 
            sunlightExposure.value AS Sunlight_Exposure, 
            rainfall.value AS Rainfall, 
            nitrogen.value AS Nitrogen, 
            phosphorus.value AS Phosphorus, 
            potassium.value AS Potassium, 
            zinc.value AS Zinc, 
            iron.value AS Iron, 
            magnesium.value AS Magnesium, 
            sodium.value AS Sodium, 
            seedVigorIndex.value AS Seed_Vigor_Index, 
            germinationRate.value AS Germination_Rate, 
            diseaseResistance.value AS Disease_Resistance, 
            pestResistance.value AS Pest_Resistance, 
            plantingDepth.value AS Planting_Depth, 
            seedSpacing.value AS Seed_Spacing, 
            vegetationIndex.value AS Vegetation_Index, 
            seedViability.value AS Seed_Viability, 
            seedMoistureContent.value AS Seed_Moisture_Content, 
            heatStressIndex.value AS Heat_Stress_Index, 
            waterStressIndex.value AS Water_Stress_Index, 
            salinityStressIndex.value AS Salinity_Stress_Index, 
            rootGrowthRate.value AS Root_Growth_Rate, 
            shootEmergenceTime.value AS Shoot_Emergence_Time, 
            soilRespirationRate.value AS Soil_Respiration_Rate, 
            organicMatterContent.value AS Organic_Matter_Content, 
            cationExchangeCapacity.value AS Cation_Exchange_Capacity, 
            electricalConductivity.value AS Electrical_Conductivity, 
            windSpeed.value AS Wind_Speed, 
            cloudCover.value AS Cloud_Cover, 
            radiationExposure.value AS Radiation_Exposure, 
            initialFertilizerApplication.value AS Initial_Fertilizer_Application, 
            yieldNode.value AS Yield
        """
        
        # Execute the query and fetch the results
        results = neo4j_conn.execute_query(export_query)
    
        df_export = pd.DataFrame(results) if results else pd.DataFrame()

        # Return the DataFrame (instead of writing to a file)
        return df_export

    except Exception as e:
        print(f"Error exporting data: {e}")
        return pd.DataFrame()

uri = "bolt://192.168.0.36:7687"
user = "neo4j"  
password = "Naveen1@"
db_name="Germination"
neo4j_conn = Neo4jConnection(uri, user, password,db_name="Germination")


def preprocess(df):
    df = pd.read_csv('germination_stage_export.csv')
    df['Date'] = pd.to_datetime(df['Date'], format="%d/%m/%Y", dayfirst=False)
    df1 = df.copy()
    df1['Day'] = df['Date'].dt.day
    df1['Month'] = df['Date'].dt.month
    df1['Year'] = df['Date'].dt.year
    df1.drop(columns="Date",axis=1,inplace=True)
    column_order = ['Day', 'Month', 'Year'] + [col for col in df1.columns if col not in ['Day', 'Month', 'Year']]
    df1 = df1[column_order]

    cat = df1.select_dtypes(include="object")
    num=df1.select_dtypes(include=["float","int"])

    Label=preprocessing.LabelEncoder()
    for col in cat.columns:        
        df1[col] = Label.fit_transform(df1[col])

    plt.boxplot(df1)
    plt.title('Boxplot Before Outlier removal') 
    plt.show()

    def iqr_bounds(column):
        q1 = df1[column].quantile(0.25)
        q3 = df1[column].quantile(0.75)
        iqr = q3 - q1
        low_bound = q1 - 1.5 * iqr
        high_bound = q3 + 1.5 * iqr
        return [low_bound, high_bound]

    while True:
        previous_shape = df1.shape
        for col in df1.select_dtypes(include=["float", "int"]).columns:
            low, high = iqr_bounds(col)
            df1 = df1[(df1[col] >= low) & (df1[col] <= high)]
        current_shape = df1.shape
        print(f'Shape after removing df1liers: {current_shape}')
        if previous_shape == current_shape:
            break

    plt.boxplot(df1)
    plt.title('Boxplot After Outlier removal') 
    plt.show()
    pickle.dump(df1,open(f"{db_name}.pkl","wb"))

def read_pickle(file_name):
    try:
        with open(file_name, 'rb') as file:
            data = pickle.load(file)

        if isinstance(data, pd.DataFrame):
            print("Available columns (keys):", list(data.keys()),"\n")
            column = input("Enter the column : ")
            
            if column in data:
                return data[column]
            else:
                return "Column not found."
        else:
            print(type(data))
            return "The data is not structured as a dictionary."
    except Exception as e:
        return f"An error occurred: {e}"





data=export_germination_relationships_from_neo4j(neo4j_conn)
data.to_csv(f"{db_name}.csv",index=False)
preprocess(data)
print(read_pickle(f"{db_name}.pkl"))
