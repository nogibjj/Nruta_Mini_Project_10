[![CI](https://github.com/nogibjj/Nruta_Mini_Project_10/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/Nruta_Mini_Project_10/actions/workflows/cicd.yml)

# IDS 706 Mini Project 10 - PySpark Data Processing

### 🏗️ Requirements
- Use PySpark to perform data processing on a large dataset
- Include at least one Spark SQL query and one data transformation

### 📂 Project Structure
```
├── .devcontainer
│   └── Dockerfile
│   └── devcontainer.json
├── Makefile
├── README.md
├── data
│   └── biopics.csv
├── main.py
├── mylib
│   ├── __init__.py
│   └── lib.py
├── pyspark_output.md
├── requirements.txt
└── test_main.py
```

### 🛠️ Setup Instructions
#### 1. Clone the Repository
```
git clone https://github.com/nogibjj/Nruta_Mini_Project_10.git
cd Nruta_Mini_Project_10
```

#### 2. Install Dependencies
```
pip install -r requirements.txt
```

#### 3. Download and set up Apache Spark.
- Download Apache Spark and choose the version compatible with your project.
- Extract the Spark package and set the environment variables if necessary. Refer to the Spark documentation for guidance.

#### 4.	Run the Project:
Refer to the Usage section for running specific scripts or modules.

### 📊 Dataset Description
The data for this project comes from the biopics.csv dataset provided by FiveThirtyEight.

The dataset has the following features:
- title
- country
- year_release
- box_office
- director
- number_of_subjects
- subject
- type_of_subject
- subject_race
- subject_sex
- lead_actor_actress

### 🗃️ Spark SQL Query
I constructed the following query to analyze the average release year of the biopics alongside the number of subjects they feature:
``` SELECT * FROM biopics WHERE number_of_subjects = 4 ;```
