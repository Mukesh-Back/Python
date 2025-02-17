import pandas as pd
import random


indian_cities = [
    "Mumbai", "Chennai", "Delhi", "Bangalore", "Hyderabad", "Kolkata", "Pune", "Ahmedabad", "Jaipur",
    "Surat", "Lucknow", "Kanpur", "Nagpur", "Indore", "Thane", "Bhopal", "Visakhapatnam", "Vadodara",
    "Patna", "Ludhiana", "Agra", "Nashik", "Faridabad", "Meerut", "Rajkot", "Varanasi", "Srinagar",
    "Aurangabad", "Dhanbad", "Amritsar", "Ranchi", "Jodhpur", "Chandigarh", "Guwahati", "Coimbatore",
    "Mysore", "Gwalior", "Jabalpur", "Tiruchirappalli", "Bareilly", "Aligarh", "Moradabad", "Jalandhar",
    "Bhubaneswar", "Salem", "Warangal", "Guntur", "Noida", "Kochi", "Solapur", "Hubli", "Tiruppur",
    "Madurai", "Raipur", "Kolhapur", "Thrissur", "Gorakhpur", "Mangalore", "Siliguri", "Dehradun",
    "Ajmer", "Nanded", "Udaipur", "Rourkela", "Jamnagar", "Bokaro", "Kozhikode", "Bhiwandi", "Cuttack",
    "Durgapur", "Dindigul", "Bellary", "Malegaon", "Tirupati", "Saharanpur", "Kollam", "Jamnagar",
    "Thiruvananthapuram", "Bilaspur", "Jhansi", "Karnal", "Shimla", "Darjeeling", "Pondicherry", "Puri",
    "Nellore", "Panipat", "Mathura", "Hosur", "Tuticorin", "Ooty", "Vellore", "Kanyakumari", "Tirunelveli",
    "Erode", "Kumbakonam", "Nizamabad", "Latur", "Sambalpur", "Akola", "Muzaffarpur", "Bhagalpur", "Bhatinda",
    "Ambala", "Rishikesh", "Haridwar", "Ujjain", "Kharagpur", "Haldia", "Kharar", "Panaji", "Margao",
    "Porvorim", "Mapusa", "Imphal", "Shillong", "Aizawl", "Agartala", "Gangtok", "Itanagar", "Dimapur",
    "Kohima", "Naharlagun", "Pasighat", "Ziro", "Tawang", "Shillong", "Cherrapunji", "Silchar", "Tezpur",
    "Jorhat", "Dibrugarh", "Nagaon", "Lakhimpur", "Karimganj", "Hailakandi", "Tinsukia", "Haflong",
    "Golaghat", "Majuli", "Sivasagar", "Rangia", "Bongaigaon", "Dhubri", "Goalpara", "Barpeta",
    "Nalbari", "Morigaon", "Kokrajhar", "Dhemaji", "Dibrugarh", "Jorhat", "Diphu", "Karbi", "Anglong",
    "Chirang", "Udalguri", "Sonitpur", "Kamrup", "Hojai", "Biswanath", "Baksa", "Tamulpur", "Manipur",
    "Tripura", "Mizoram", "Nagaland", "Meghalaya", "Arunachal Pradesh", "Sikkim", "Lakshadweep",
    "Andaman", "Nicobar", "Rameswaram", "Mahabalipuram", "Kancheepuram", "Coonoor", "Kodaikanal",
    "Madikeri", "Chikmagalur", "Yercaud", "Hampi", "Anegundi", "Pattadakal", "Badami", "Bijapur",
    "Belur", "Halebidu", "Shravanabelagola", "Somnath", "Dwarka", "Porbandar", "Mandvi", "Bhuj",
    "Junagadh", "Patan", "Modhera", "Champaner", "Kevadia", "Gandhinagar", "Surendranagar", "Morbi",
    "Wankaner", "Jamnagar", "Dwarka", "Rajkot", "Junagadh", "Amreli", "Bhavnagar", "Palitana",
    "Mahuva", "Porbandar", "Veraval", "Somnath", "Kodinar", "Una", "Talala", "Diu", "Daman",
    "Silvassa", "Dadra", "Nagar Haveli", "Karwar", "Goa", "Alleppey", "Kumarakom", "Munnar",
    "Wayanad", "Kumarakom", "Thrissur", "Kalady", "Ernakulam", "Idukki", "Thekkady", "Vagamon",
    "Bekal", "Kasargod", "Kannur", "Thalassery", "Kozhikode", "Palakkad", "Malappuram", "Kollam",
    "Trivandrum", "Pathanamthitta", "Alappuzha", "Cherthala", "Kottayam", "Pala", "Muvattupuzha",
    "Piravom", "Thodupuzha", "Kothamangalam", "Chalakudy", "Thriprayar", "Guruvayur", "Kunnamkulam",
    "Kodungallur", "Irinjalakuda", "Chavakkad", "Ponnani", "Perinthalmanna", "Manjeri", "Malappuram",
    "Tirur", "Parappanangadi", "Kottakkal", "Tanur", "Pallikadavu", "Chalakkudy", "Nellikuzhi",
    "Panangad", "Aroor", "Kodungallur", "Mala", "Ollur", "Palayoor", "Mathilakom", "Chendamangalam",
    "Ernakulam", "Kalady", "Edappally", "Kadavanthra", "Tripunithura", "Vyttila", "Thevara",
    "Mattancherry", "Fort Kochi", "Bolgatty", "Vypeen", "Cherai", "Kumbalangi", "Maradu", "Kakkanad",
    "Palarivattom", "Kalamassery", "Aluva", "Angamaly", "Perumbavoor", "Muvattupuzha", "Thodupuzha",
    "Kothamangalam", "Malappuram", "Tirur", "Kottakkal", "Thalassery", "Kannur", "Kasargod", "Manjeshwar"
]


random_cities = random.sample(indian_cities, 250)
case_numbers_random = [f"CA{i+2}" for i in range(250)]


crime_dataset = pd.DataFrame({
    "City Name": random_cities,
    "Case Number": case_numbers_random
})

transposed_dataset = crime_dataset.pivot(columns="City Name", values="Case Number")


transposed_dataset.to_csv("Transposed_Crime_Rate_Dataset.csv", index=False)

print("Transposed dataset saved as 'Transposed_Crime_Rate_Dataset.csv'.")