import random
import pandas as pd

def generate_value(min_value, max_value):
    return random.uniform(min_value, max_value)

def generate_choice(choices):
    return random.choice(choices)


def generate_hierarchical_data():
    season = generate_choice(["Kharif", "Radi", "Zaid"])
    if season == "Kharif":
        #Sowing
        Duration=[90,120]
        Fertilize_value = [[30,40], [0,1.5], [40,60], [10,15], [40,60]]  # Fertilizer Exposure: Phosphate, Sodium, Nitrogen, Magnesium, Potassium
        Micronutrient_value = [[1,2], [50,100], [0.1,0.3], [2,3], [20,40], [0.05,0.1], [2,5]]  # Micronutrient Exposure: Boron, Chlorine, Copper, Iron, Manganese, Molybdenum, Zinc
        Vegetation_Index = [0.2,0.4]
        SoilMoisture_Content=[5,10]
        SoilTemperature=[25,30]
        AmbientTemperature = [28,32]
        Humidity=[80,90]
        NitrogenAvailability=[50,150]
        PlantDensity=[10 , 20]
        Temperature=[25,30]
        Photoperiod=[12,13]



        #Germination
        G_Fertilize_value = [[40,50], [0,1.5], [20,30], [20,25], [30,50]]  # Fertilizer Exposure: Phosphate, Sodium, Nitrogen, Magnesium, Potassium
        G_Micronutrient_value = [[1,2], [50,100], [0.1,0.3], [2,3], [20,40], [0.05,0.1], [2,5]]  # Micronutrient Exposure: Boron, Chlorine, Copper, Iron, Manganese, Molybdenum, Zinc
        G_PanicleSpikeletCount = [[250,300],[150,200]]
        G_Sunlight=[6,8]
        G_RadiationExposure=[1000,2000]
        G_SoilMoisture_Content=[5,10]
        G_InorganicAnionExposure=[0,1.5]
        G_AmbientTemperature=[27,32]
        G_Humidity=[70,80]
        

        # Tillering
        T_Fertilize_value = [[10, 20], [0, 1.5], [20, 30], [10, 15], [30, 50]]  # Phosphate, Sodium, Nitrogen, Magnesium, Potassium
        T_Micronutrient_value = [[1, 2], [50, 100], [0.1, 0.3], [2, 3], [20, 40], [0.05, 0.1], [2, 5]]  # Boron, Chlorine, Copper, Iron, Manganese, Molybdenum, Zinc
        T_SoilMoistureContent = [5, 10]
        T_RadiationExposure = [1000, 1500]
        T_InsectPlantExposure = [0, 1]
        T_InsecticideExposure = [0, 1.5]
        T_VascularLeafReceptoractivity = [0.5, 1]
        T_Moisture = [5, 10]
        T_AmbientTemperature = [28, 32]
        T_Humidity = [70, 80]

        # Stem Elongation
        S_Fertilize_value = [[20, 30], [0, 1.5], [30, 40], [10, 15], [30, 50]]  # Phosphate, Sodium, Nitrogen, Magnesium, Potassium
        S_Micronutrient_value = [[1, 2], [50, 100], [0.1, 0.3], [2, 3], [20, 40], [0.05, 0.1], [2, 5]]  # Boron, Chlorine, Copper, Iron, Manganese, Molybdenum, Zinc
        S_SoilMoistureContent = [5, 10]
        S_VascularLeafReceptoractivity = [0.5, 1]
        S_Moisture = [5, 10]
        S_RadiationExposure = [1000, 1500]
        S_AmbientTemperature = [28, 32]
        S_Humidity = [70, 80]

        # Flowering
        F_Fertilize_value = [[30, 40], [0, 1.5], [30, 50], [10, 15], [30, 50]]  # Phosphate, Sodium, Nitrogen, Magnesium, Potassium
        F_Micronutrient_value = [[1, 2], [50, 100], [0.1, 0.3], [2, 3], [20, 40], [0.05, 0.1], [2, 5]]  # Boron, Chlorine, Copper, Iron, Manganese, Molybdenum, Zinc
        F_SoilMoistureContent = [5, 10]
        F_Sunlight = [6, 8]
        F_AmbientTemperature = [28, 32]
        F_RadiationExposure = [1000, 1500]
        F_Humidity = [70, 80]
        F_Precipitation = [0, 20]
        F_Windspeed = [5, 10]

        # Grain Filling & Maturation
        GF_Fertilize_value = [[40, 50], [0, 1.5], [30, 50], [10, 15], [40, 60]]  # Phosphate, Sodium, Nitrogen, Magnesium, Potassium
        GF_Micronutrient_value = [[1, 2], [50, 100], [0.1, 0.3], [2, 3], [20, 40], [0.05, 0.1], [2, 5]]  # Boron, Chlorine, Copper, Iron, Manganese, Molybdenum, Zinc
        GF_SoilMoistureContent = [5, 10]
        GF_InsecticideExposure = [0, 1.5]
        GF_EcologicalExposure = [0, 1]
        GF_Sunlight = [6, 8]
        GF_AmbientTemperature = [28, 32]
        GF_RadiationExposure = [1000, 1500]
        GF_Humidity = [70, 80]
        GF_Precipitation = [0, 20]
        GF_Windspeed = [5, 10]

        # Harvesting
        
        H_Soilquality=[5, 8]
        H_moisture=[15, 25] 
        H_Sunlight=[5,7]


# Sowing Stage
    elif season == "Zaid":
        # Sowing
        Duration = [90, 110]
        Fertilize_value = [[20, 30], [0, 1.5], [20, 30], [10, 15], [30, 50]]  # Phosphate, Sodium, Nitrogen, Magnesium, Potassium
        Micronutrient_value = [[0.5, 1.5], [10, 20], [0.1, 0.3], [2, 3], [40, 50], [0.05, 0.1], [2, 5]]  # Boron, Chlorine, Copper, Iron, Manganese, Molybdenum, Zinc
        Vegetation_Index = [0.3, 0.5]
        SoilMoisture_Content = [5, 10]
        SoilTemperature = [25, 30]
        AmbientTemperature = [28, 32]
        Humidity = [80, 90]
        NitrogenAvailability = [50, 130]
        PlantDensity = [10, 22]
        Temperature = [30, 35]
        Photoperiod = [12, 13]

        # Germination Stage
        G_Fertilize_value = [[40, 50], [0, 1.5], [20, 30], [20, 25], [30, 50]]  # Phosphate, Sodium, Nitrogen, Magnesium, Potassium
        G_Micronutrient_value = [[1, 2], [50, 100], [0.1, 0.3], [2, 3], [20, 40], [0.05, 0.1], [2, 5]]  # Boron, Chlorine, Copper, Iron, Manganese, Molybdenum, Zinc
        G_PanicleSpikeletCount = [[250, 300], [150, 200]]
        G_Sunlight = [6, 8]
        G_RadiationExposure = [0.5, 1.0]  
        G_SoilMoisture_Content = [7, 12]  
        G_InorganicAnionExposure = [0.05, 0.15]  
        G_AmbientTemperature = [28, 32] 
        G_Humidity = [75, 85] 

        # Tillering Stage
        T_Fertilize_value = [[10, 20], [0, 1.5], [10, 20], [5, 10], [20, 40]]  # Phosphate, Sodium, Nitrogen, Magnesium, Potassium
        T_Micronutrient_value = [[0.5, 1.0], [30, 50], [0.05, 0.15], [1, 2], [10, 20], [0.01, 0.05], [1, 3]]  # Boron, Chlorine, Copper, Iron, Manganese, Molybdenum, Zinc
        T_SoilMoistureContent = [8, 12]  
        T_RadiationExposure = [1.0, 1.5] 
        T_InsectPlantExposure = [1, 5]  
        T_InsecticideExposure = [0, 2]  
        T_VascularLeafReceptoractivity = [0.5, 0.8]  
        T_Moisture = [8, 15]  
        T_RadiationExposure = [0.9, 1.2] 
        T_AmbientTemperature = [28, 30]  
        T_Humidity = [70, 80] 

        # Stem Elongation Stage
        S_Fertilize_value = [[5, 10], [0, 1.0], [5, 10], [2, 5], [10, 20]]  # Phosphate, Sodium, Nitrogen, Magnesium, Potassium
        S_Micronutrient_value = [[0.5, 1.0], [15, 30], [0.05, 0.1], [1, 2], [5, 10], [0.01, 0.05], [1, 2]]  # Boron, Chlorine, Copper, Iron, Manganese, Molybdenum, Zinc
        S_SoilMoistureContent = [10, 15]  
        S_VascularLeafReceptoractivity = [0.5, 0.7]  
        S_Moisture = [10, 20]  
        S_RadiationExposure = [1.0, 1.5]  
        S_AmbientTemperature = [30, 32]  
        S_Humidity = [70, 80]  

        # Flowing Stage
        F_Fertilize_value = [[5, 15], [0, 1.0], [10, 20], [5, 10], [10, 20]]  # Phosphate, Sodium, Nitrogen, Magnesium, Potassium
        F_Micronutrient_value = [[0.5, 1.0], [10, 20], [0.05, 0.1], [1, 2], [5, 10], [0.01, 0.05], [1, 2]]  # Boron, Chlorine, Copper, Iron, Manganese, Molybdenum, Zinc
        F_SoilMoistureContent = [10, 15] 
        F_Sunlight = [6, 8]  
        F_AmbientTemperature = [30, 32]  
        F_RadiationExposure = [1.5, 2.0]  
        F_Humidity = [75, 85] 
        F_Precipitation = [5, 20]  
        F_Windspeed = [5, 10]  

        # Grain Filling & Maturation Stage
        GF_Fertilize_value = [[10, 15], [0, 1.5], [15, 25], [5, 10], [15, 25]]  # Phosphate, Sodium, Nitrogen, Magnesium, Potassium
        GF_Micronutrient_value = [[0.5, 1.0], [5, 10], [0.05, 0.1], [1, 2], [10, 20], [0.01, 0.05], [1, 2]]  # Boron, Chlorine, Copper, Iron, Manganese, Molybdenum, Zinc
        GF_SoilMoistureContent = [10, 15]  
        GF_InsecticideExposure = [0, 2]  
        GF_EcologicalExposure = [0, 1]  
        GF_Sunlight = [6, 8] 
        GF_AmbientTemperature = [30, 32]  
        GF_RadiationExposure = [1.5, 2.0]  
        GF_Humidity = [75, 85]  
        GF_Precipitation = [5, 20]  
        GF_Windspeed = [5, 10]  

        # Harvesting Stage
        H_Soilquality = [8, 10]  
        H_moisture = [20, 30]  
        H_Sunlight = [7, 10]  


        



    elif season == "Radi":
        
        # Sowing Stage
        Duration = [120, 140]
        Fertilize_value = [[20, 30], [0, 1.0], [20, 30], [10, 15], [25, 50]]  # Phosphate, Sodium, Nitrogen, Magnesium, Potassium
        Micronutrient_value = [[0.5, 1.0], [10, 20], [0.1, 0.3], [2, 3], [40, 50], [0.05, 0.1], [2, 5]]  # Boron, Chlorine, Copper, Iron, Manganese, Molybdenum, Zinc
        Vegetation_Index = [0.4, 0.6]  
        SoilMoisture_Content = [6, 12]  
        SoilTemperature = [18, 25] 
        AmbientTemperature = [20, 28] 
        Humidity = [70, 85]
        NitrogenAvailability = [40, 120]  
        PlantDensity = [12, 18] 
        Temperature = [18, 25] 
        Photoperiod = [10, 12]  

        # Germination Stage
        G_Fertilize_value = [[30, 40], [0, 1.0], [20, 30], [15, 20], [25, 40]]  # Phosphate, Sodium, Nitrogen, Magnesium, Potassium
        G_Micronutrient_value = [[1, 2], [40, 80], [0.1, 0.3], [2, 3], [15, 30], [0.05, 0.1], [2, 5]]  # Boron, Chlorine, Copper, Iron, Manganese, Molybdenum, Zinc
        G_PanicleSpikeletCount = [[200, 250], [100, 150]]
        G_Sunlight = [6, 8]  
        G_RadiationExposure = [0.5, 1.0]  
        G_SoilMoisture_Content = [6, 10]  
        G_InorganicAnionExposure = [0.05, 0.1]  
        G_AmbientTemperature = [20, 28] 
        G_Humidity = [75, 85]

        # Tillering Stage
        T_Fertilize_value = [[15, 25], [0, 1.0], [10, 20], [5, 10], [15, 30]]  # Phosphate, Sodium, Nitrogen, Magnesium, Potassium
        T_Micronutrient_value = [[0.5, 1.0], [25, 50], [0.05, 0.15], [1, 2], [8, 15], [0.01, 0.05], [1, 3]]  # Boron, Chlorine, Copper, Iron, Manganese, Molybdenum, Zinc
        T_SoilMoistureContent = [7, 12]  
        T_RadiationExposure = [1.0, 1.5]  
        T_InsectPlantExposure = [1, 3] 
        T_InsecticideExposure = [0, 1]  
        T_VascularLeafReceptoractivity = [0.5, 0.7]  
        T_Moisture = [7, 12] 
        T_RadiationExposure = [1.0, 1.5]  
        T_AmbientTemperature = [20, 25] 
        T_Humidity = [70, 80]

        # Stem Elongation Stage
        S_Fertilize_value = [[5, 10], [0, 1.0], [5, 10], [2, 5], [10, 20]]  # Phosphate, Sodium, Nitrogen, Magnesium, Potassium
        S_Micronutrient_value = [[0.5, 1.0], [15, 30], [0.05, 0.1], [1, 2], [5, 10], [0.01, 0.05], [1, 2]]  # Boron, Chlorine, Copper, Iron, Manganese, Molybdenum, Zinc
        S_SoilMoistureContent = [8, 12]  
        S_VascularLeafReceptoractivity = [0.5, 0.7]  
        S_Moisture = [8, 15]  
        S_RadiationExposure = [1.0, 1.5]  
        S_AmbientTemperature = [20, 25] 
        S_Humidity = [75, 85]

        # Flowing Stage
        F_Fertilize_value = [[10, 20], [0, 1.5], [15, 25], [5, 10], [10, 20]]  # Phosphate, Sodium, Nitrogen, Magnesium, Potassium
        F_Micronutrient_value = [[0.5, 1.0], [10, 20], [0.05, 0.1], [1, 2], [5, 10], [0.01, 0.05], [1, 2]]  # Boron, Chlorine, Copper, Iron, Manganese, Molybdenum, Zinc
        F_SoilMoistureContent = [10, 15]  
        F_Sunlight = [6, 8]  
        F_AmbientTemperature = [20, 25] 
        F_RadiationExposure = [1.5, 2.0]  
        F_Humidity = [75, 85]
        F_Precipitation = [10, 30]  
        F_Windspeed = [5, 15]  

        # Grain Filling & Maturation Stage
        GF_Fertilize_value = [[10, 20], [0, 1.5], [15, 25], [5, 10], [15, 30]]  # Phosphate, Sodium, Nitrogen, Magnesium, Potassium
        GF_Micronutrient_value = [[0.5, 1.0], [5, 10], [0.05, 0.1], [1, 2], [10, 20], [0.01, 0.05], [1, 2]]  # Boron, Chlorine, Copper, Iron, Manganese, Molybdenum, Zinc
        GF_SoilMoistureContent = [10, 15] 
        GF_InsecticideExposure = [0, 2]  
        GF_EcologicalExposure = [0, 1]
        GF_Sunlight = [6, 8]  
        GF_AmbientTemperature = [20, 25] 
        GF_RadiationExposure = [1.5, 2.0]  
        GF_Humidity = [75, 85]
        GF_Precipitation = [5, 20]  
        GF_Windspeed = [5, 10]  

        # Harvesting Stage
        H_Soilquality = [5, 7]  
        H_moisture = [10, 15] 
        H_Sunlight = [6, 8]  



        
    
    data = {
        "Rice": {
            "Sowing": {
                "Crop Variety": generate_choice(["Short", "Long"]),
                "Duration":generate_value(*Duration),
                "Season": season,
                "Nitrogen Availability":generate_value(*NitrogenAvailability),
                "Plant Density at Lakhs":generate_value(*PlantDensity),
                "Temperature":generate_value(*Temperature),
                "Photoperiod":generate_value(*Photoperiod),
                "Plant Quality Trait": generate_choice(["Color", "Purity", "Grain Shape", "Aroma"]),
                "Plant Vigor Trait": generate_choice(["Healthy", "Vigor", "Growth"]),
                "Yield Trait": generate_choice(["High Yield", "Medium Yield", "Hybrid"]),
                "Stress Trait": generate_choice(["Heat Stress", "Salinity Stress", "Heavy Metals Stress", "Cold Stress", "Drought Stress"]),
                "Fertilize_Phosphate (Per Acre on Gram)": generate_value(*Fertilize_value[0]),
                "Fertilize_Sodium (Per Acre on Gram)": generate_value(*Fertilize_value[1]),
                "Fertilize_Nitrogen (Per Acre on Gram)": generate_value(*Fertilize_value[2]),
                "Fertilize_Magnesium (Per Acre on Gram)": generate_value(*Fertilize_value[3]),
                "Fertilize_Potassium (Per Acre on Gram)": generate_value(*Fertilize_value[4]),
                "Vegetation Index": generate_value(*Vegetation_Index),
                "Micronutrient_Boron Per Year at Acre": generate_value(*Micronutrient_value[0]),
                "Micronutrient_Chlorine Per Acre": generate_value(*Micronutrient_value[1]),
                "Micronutrient_Copper Per Acre": generate_value(*Micronutrient_value[2]),
                "Micronutrient_Iron Per Acre": generate_value(*Micronutrient_value[3]),
                "Micronutrient_Manganese Per Acre": generate_value(*Micronutrient_value[4]),
                "Micronutrient_Molybdenum Per Acre": generate_value(*Micronutrient_value[5]),
                "Micronutrient_Zinc Per Acre": generate_value(*Micronutrient_value[6]),
                "Soil Moisture Content":generate_value(*SoilMoisture_Content),
                "Soil Temperature":generate_value(*SoilTemperature),
                "Ambient Temperature (Summer)":generate_value(*AmbientTemperature),
                "Humidity":generate_value(*Humidity)


            },
            "Germination": {
                "Nitrogen Availability":generate_value(*NitrogenAvailability),
                "Plant Density at Lakhs":generate_value(*PlantDensity),
                "Temperature":generate_value(*Temperature),
                "Photoperiod":generate_value(*Photoperiod),
                "Plant Exposure":generate_choice(["Direct sunlight exposure","Pesticide or chemical exposure"]),
                "Abiotic Exposure":generate_choice(["Temperature fluctuations","Water stress (drought or flooding)"]),
                "Biotic Exposure":generate_choice(["Herbivory by animals","Competition with other plant species"]),
                "Ecological Exposure":generate_choice(["Pollution from industrial runoff","Habitat fragmentation due to human development"]),
                "Fertilize_Phosphate (Per Acre on Gram)": generate_value(*G_Fertilize_value[0]),
                "Fertilize_Sodium (Per Acre on Gram)": generate_value(*G_Fertilize_value[1]),
                "Fertilize_Nitrogen (Per Acre on Gram)": generate_value(*G_Fertilize_value[2]),
                "Fertilize_Magnesium (Per Acre on Gram)": generate_value(*G_Fertilize_value[3]),
                "Fertilize_Potassium (Per Acre on Gram)": generate_value(*G_Fertilize_value[4]),
                "Fungicide":generate_choice(["mancozeb", "chlorothalonil","Organic fungicides"]),
                "Vegetation Index": generate_value(*Vegetation_Index),
                "Panicle & Spikelet Count":(generate_value(*G_PanicleSpikeletCount[0]),*G_PanicleSpikeletCount[1]),
                "Micronutrient_Boron Per Year at Acre": generate_value(*G_Micronutrient_value[0]),
                "Micronutrient_Chlorine Per Acre": generate_value(*G_Micronutrient_value[1]),
                "Micronutrient_Copper Per Acre": generate_value(*G_Micronutrient_value[2]),
                "Micronutrient_Iron Per Acre": generate_value(*G_Micronutrient_value[3]),
                "Micronutrient_Manganese Per Acre": generate_value(*G_Micronutrient_value[4]),
                "Micronutrient_Molybdenum Per Acre": generate_value(*G_Micronutrient_value[5]),
                "Micronutrient_Zinc Per Acre": generate_value(*G_Micronutrient_value[6]),
                "Growth Harmone Exposure":generate_choice(["auxins","cytokinins","gibberellins","abscisic acid"]),
                "Sunlight":generate_value(*G_Sunlight),
                "dsRNA Virus exposure":generate_choice(["plant-infecting dsRNA viruses","animal-infecting dsRNA viruses","synthetic dsRNA viruses"]),
                "Radiation Exposure":generate_value(*G_RadiationExposure),
                "Soil Moisture Content":generate_value(*G_SoilMoisture_Content),
                "Inorganic Anion Exposure":generate_value(*G_InorganicAnionExposure),
                "Ambient Temperature":generate_value(*G_AmbientTemperature),
                "Humidity":generate_value(*G_Humidity)
            },

            "Tillering": {
                "Nitrogen Availability":generate_value(*NitrogenAvailability),
                "Plant Density at Lakhs":generate_value(*PlantDensity),
                "Temperature":generate_value(*Temperature),
                "Photoperiod":generate_value(*Photoperiod),
                "Soil Moisture Content":generate_value(*T_SoilMoistureContent),
                "Radiation Exposure":generate_value(*T_RadiationExposure),
                "Insect Plant Exposure":generate_value(*T_InsectPlantExposure),
                "Insecticide Exposure":generate_value(*T_InsecticideExposure),
                "Growth Harmone Exposure":generate_choice(["auxins (promote cell elongation)","cytokinins (promote cell division)","gibberellins (promote stem elongation and flowering)","abscisic acid (involved in stress response)","ethylene (regulates fruit ripening and stress responses)"]),
                "Herbicide Exposure":generate_choice(["glyphosate","atrazine","2,4-D (auxin mimic)","dicamba","paraquat"]),
                "Abiotic Exposure":generate_choice(["temperature extremes ","drought conditions","salinity","heavy metals","high UV radiation"]),
                "Biotic Exposure":generate_choice(["herbivory by insects","fungal pathogens","bacterial infections","competition with other plant species","mutualistic microorganisms "]),
                "Vascular Leaf & Receptor activity":generate_value(*T_VascularLeafReceptoractivity),
                "dsRNA Virus exposure":generate_choice(["plant-infecting dsRNA viruses","animal-infecting dsRNA viruses","synthetic dsRNA viruses"]),
                "Fertilize_Phosphate (Per Acre on Gram)": generate_value(*T_Fertilize_value[0]),
                "Fertilize_Sodium (Per Acre on Gram)": generate_value(*T_Fertilize_value[1]),
                "Fertilize_Nitrogen (Per Acre on Gram)": generate_value(*T_Fertilize_value[2]),
                "Fertilize_Magnesium (Per Acre on Gram)": generate_value(*T_Fertilize_value[3]),
                "Fertilize_Potassium (Per Acre on Gram)": generate_value(*T_Fertilize_value[4]),
                "Vegetation Index": generate_value(*Vegetation_Index),
                "Micronutrient_Boron Per Year at Acre": generate_value(*T_Micronutrient_value[0]),
                "Micronutrient_Chlorine Per Acre": generate_value(*T_Micronutrient_value[1]),
                "Micronutrient_Copper Per Acre": generate_value(*T_Micronutrient_value[2]),
                "Micronutrient_Iron Per Acre": generate_value(*T_Micronutrient_value[3]),
                "Micronutrient_Manganese Per Acre": generate_value(*T_Micronutrient_value[4]),
                "Micronutrient_Molybdenum Per Acre": generate_value(*T_Micronutrient_value[5]),
                "Micronutrient_Zinc Per Acre": generate_value(*T_Micronutrient_value[6]),
                "Fungicide":generate_choice(["mancozeb", "chlorothalonil","Organic fungicides"]),
                "Moisture":generate_value(*T_Moisture),
                "Radiation Exposure":generate_value(*T_RadiationExposure),
                "Ambient Temperature":generate_value(*T_AmbientTemperature),
                "Humidity":generate_value(*T_Humidity),
                
            },
            "Stem Elongation": {
                "Nitrogen Availability":generate_value(*NitrogenAvailability),
                "Plant Density at Lakhs":generate_value(*PlantDensity),
                "Temperature":generate_value(*Temperature),
                "Photoperiod":generate_value(*Photoperiod),
                "Soil Moisture Content":generate_value(*S_SoilMoistureContent),
                "Herbicide Exposure":generate_choice(["glyphosate","atrazine","2,4-D (auxin mimic)","dicamba","paraquat"]),
                "Abiotic Exposure":generate_choice(["temperature extremes ","drought conditions","salinity","heavy metals","high UV radiation"]),
                "Biotic Exposure":generate_choice(["herbivory by insects","fungal pathogens","bacterial infections","competition with other plant species","mutualistic microorganisms "]),
                "dsRNA Virus exposure":generate_choice(["plant-infecting dsRNA viruses","animal-infecting dsRNA viruses","synthetic dsRNA viruses"]),
                "Vascular Leaf & Receptor activity":generate_value(*S_VascularLeafReceptoractivity),
                "Fertilize_Phosphate (Per Acre on Gram)": generate_value(*S_Fertilize_value[0]),
                "Fertilize_Sodium (Per Acre on Gram)": generate_value(*S_Fertilize_value[1]),
                "Fertilize_Nitrogen (Per Acre on Gram)": generate_value(*S_Fertilize_value[2]),
                "Fertilize_Magnesium (Per Acre on Gram)": generate_value(*S_Fertilize_value[3]),
                "Fertilize_Potassium (Per Acre on Gram)": generate_value(*S_Fertilize_value[4]),
                "Vegetation Index": generate_value(*Vegetation_Index),
                "Micronutrient_Boron Per Year at Acre": generate_value(*S_Micronutrient_value[0]),
                "Micronutrient_Chlorine Per Acre": generate_value(*S_Micronutrient_value[1]),
                "Micronutrient_Copper Per Acre": generate_value(*S_Micronutrient_value[2]),
                "Micronutrient_Iron Per Acre": generate_value(*S_Micronutrient_value[3]),
                "Micronutrient_Manganese Per Acre": generate_value(*S_Micronutrient_value[4]),
                "Micronutrient_Molybdenum Per Acre": generate_value(*S_Micronutrient_value[5]),
                "Micronutrient_Zinc Per Acre": generate_value(*S_Micronutrient_value[6]),
                "Fungicide":generate_choice(["mancozeb", "chlorothalonil","Organic fungicides"]),
                "Moisture":generate_value(*S_Moisture),
                "Radiation Exposure":generate_value(*S_RadiationExposure),
                "Ambient Temperature":generate_value(*S_AmbientTemperature),
                "Humidity":generate_value(*S_Humidity),




            },
            "Flowering": {
                "Nitrogen Availability":generate_value(*NitrogenAvailability),
                "Plant Density at Lakhs":generate_value(*PlantDensity),
                "Temperature":generate_value(*Temperature),
                "Photoperiod":generate_value(*Photoperiod),
                "Soil Moisture Content":generate_value(*F_SoilMoistureContent),
                "Herbicide Exposure":generate_choice(["glyphosate","atrazine","2,4-D (auxin mimic)","dicamba","paraquat"]),
                "Abiotic Exposure":generate_choice(["temperature extremes ","drought conditions","salinity","heavy metals","high UV radiation"]),
                "Biotic Exposure":generate_choice(["herbivory by insects","fungal pathogens","bacterial infections","competition with other plant species","mutualistic microorganisms "]),
                
                "dsRNA Virus exposure":generate_choice(["plant-infecting dsRNA viruses","animal-infecting dsRNA viruses","synthetic dsRNA viruses"]),
                "Fertilize_Phosphate (Per Acre on Gram)": generate_value(*F_Fertilize_value[0]),
                "Fertilize_Sodium (Per Acre on Gram)": generate_value(*F_Fertilize_value[1]),
                "Fertilize_Nitrogen (Per Acre on Gram)": generate_value(*F_Fertilize_value[2]),
                "Fertilize_Magnesium (Per Acre on Gram)": generate_value(*F_Fertilize_value[3]),
                "Fertilize_Potassium (Per Acre on Gram)": generate_value(*F_Fertilize_value[4]),
                "Vegetation Index": generate_value(*Vegetation_Index),
                "Micronutrient_Boron Per Year at Acre": generate_value(*F_Micronutrient_value[0]),
                "Micronutrient_Chlorine Per Acre": generate_value(*F_Micronutrient_value[1]),
                "Micronutrient_Copper Per Acre": generate_value(*F_Micronutrient_value[2]),
                "Micronutrient_Iron Per Acre": generate_value(*F_Micronutrient_value[3]),
                "Micronutrient_Manganese Per Acre": generate_value(*F_Micronutrient_value[4]),
                "Micronutrient_Molybdenum Per Acre": generate_value(*F_Micronutrient_value[5]),
                "Micronutrient_Zinc Per Acre": generate_value(*F_Micronutrient_value[6]),
                "Fungicide":generate_choice(["mancozeb", "chlorothalonil","Organic fungicides"]),
                "Sunlight":generate_value(*F_Sunlight),
                "Radiation Exposure":generate_value(*F_RadiationExposure),
                "Ambient Temperature":generate_value(*F_AmbientTemperature),
                "Humidity":generate_value(*F_Humidity),
                "Precipitation":generate_value(*F_Precipitation),
                "Windspeed":generate_value(*F_Windspeed),
                


            },
            "Grain Filling & Maturation": {
                "Nitrogen Availability":generate_value(*NitrogenAvailability),
                "Plant Density at Lakhs":generate_value(*PlantDensity),
                "Temperature":generate_value(*Temperature),
                "Photoperiod":generate_value(*Photoperiod),
                "Soil Moisture Content":generate_value(*GF_SoilMoistureContent),
                "Insecticide Exposure":generate_value(*GF_InsecticideExposure),
                "Ecological Exposure":generate_value(*GF_EcologicalExposure),
                
                "Fertilize_Phosphate (Per Acre on Gram)": generate_value(*GF_Fertilize_value[0]),
                "Fertilize_Sodium (Per Acre on Gram)": generate_value(*GF_Fertilize_value[1]),
                "Fertilize_Nitrogen (Per Acre on Gram)": generate_value(*GF_Fertilize_value[2]),
                "Fertilize_Magnesium (Per Acre on Gram)": generate_value(*GF_Fertilize_value[3]),
                "Fertilize_Potassium (Per Acre on Gram)": generate_value(*GF_Fertilize_value[4]),
                "Vegetation Index": generate_value(*Vegetation_Index),
                "Micronutrient_Boron Per Year at Acre": generate_value(*GF_Micronutrient_value[0]),
                "Micronutrient_Chlorine Per Acre": generate_value(*GF_Micronutrient_value[1]),
                "Micronutrient_Copper Per Acre": generate_value(*GF_Micronutrient_value[2]),
                "Micronutrient_Iron Per Acre": generate_value(*GF_Micronutrient_value[3]),
                "Micronutrient_Manganese Per Acre": generate_value(*GF_Micronutrient_value[4]),
                "Micronutrient_Molybdenum Per Acre": generate_value(*GF_Micronutrient_value[5]),
                "Micronutrient_Zinc Per Acre": generate_value(*GF_Micronutrient_value[6]),
                "Fungicide":generate_choice(["mancozeb", "chlorothalonil","Organic fungicides"]),
                "Sunlight":generate_value(*GF_Sunlight),
                "Radiation Exposure":generate_value(*GF_RadiationExposure),
                "Ambient Temperature":generate_value(*GF_AmbientTemperature),
                "Humidity":generate_value(*GF_Humidity),
                "Precipitation":generate_value(*GF_Precipitation),
                "Windspeed":generate_value(*GF_Windspeed),
                


            },
            "Harvesting": {
                "Nitrogen Availability":generate_value(*NitrogenAvailability),
                "Plant Density at Lakhs":generate_value(*PlantDensity),
                "Temperature":generate_value(*Temperature),
                "Photoperiod":generate_value(*Photoperiod),
                "Soil quality":generate_value(*H_Soilquality),
                "moisture":generate_value(*H_moisture),
                "Fungicide":generate_choice(["mancozeb", "chlorothalonil","Organic fungicides"]),
                "Sunlight":generate_value(*H_Sunlight)


            }
        }
    }
    return data

def flatten_data(data, parent_key='', sep='_'):
    items = []
    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_data(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

count=int(input("How Many Data You Want ? :"))

all_data = []
for _ in range(count):
    hierarchical_data = generate_hierarchical_data()
    flattened_data = flatten_data(hierarchical_data)
    all_data.append(flattened_data)


df = pd.DataFrame(all_data)

df.to_csv("C:\\Users\\visualapp\\Music\\Results\\data.csv", index=False)

print("Data saved to data.csv")
