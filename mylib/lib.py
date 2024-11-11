"""
library functions
"""

import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

from pyspark.sql.types import StructType, StructField, IntegerType, StringType


LOG_FILE = "pyspark_output.md"


def log_output(operation, output, query=None):
    """adds to a markdown file"""
    with open(LOG_FILE, "a") as file:
        file.write(f"The operation is {operation}\n\n")
        if query:
            file.write(f"The query is {query}\n\n")
        file.write("The truncated output is: \n\n")
        file.write(output)
        file.write("\n\n")


def start_spark(appName):
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark


def end_spark(spark):
    spark.stop()
    return "stopped spark session"


def extract(
    url="https://raw.githubusercontent.com/nruta-choudhari/Datasets/refs/heads/main/biopics.csv",
    file_path="data/biopics.csv",
    directory="data",
):
    """Extract a URL to a file path only if the file is a CSV"""
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Download the content from the URL
    with requests.get(url) as r:
        content = r.text

        # Check if the content is valid CSV by looking for CSV headers
        if "title" in content and "country" in content:
            with open(file_path, "w") as f:
                f.write(content)
            print(f"File successfully downloaded as {file_path}")
        else:
            print("Error: The downloaded content is not a valid CSV file.")
            # Optionally, you could handle this error by raising an exception
            raise ValueError("The downloaded file is not a valid CSV.")

    return file_path


def load_data(spark, data="data/biopics.csv", name="biopics"):
    """load data"""
    # data preprocessing by setting schema
    schema = StructType(
        [
            StructField("title", StringType(), True),
            StructField("country", StringType(), True),
            StructField("year_release", IntegerType(), True),
            StructField("box_office", StringType(), True),
            StructField("director", StringType(), True),
            StructField("number_of_subjects", IntegerType(), True),
            StructField("subject", StringType(), True),
            StructField("type_of_subject", StringType(), True),
            StructField("subject_race", StringType(), True),
            StructField("subject_sex", StringType(), True),
            StructField("lead_actor_actress", StringType(), True),
        ]
    )

    df = spark.read.option("header", "true").schema(schema).csv(data)

    # Register DataFrame as a temporary view
    df.createOrReplaceTempView(name)

    log_output("load data", df.limit(10).toPandas().to_markdown())

    return df


def query(spark, df, query, name):
    """queries using spark sql"""
    df = df.createOrReplaceTempView(name)

    log_output("query data", spark.sql(query).toPandas().to_markdown(), query)

    return spark.sql(query).show()


def describe(df):
    summary_stats_str = df.describe().toPandas().to_markdown()
    log_output("describe data", summary_stats_str)

    return df.describe().show()


def example_transform(df):
    """Example transformation for the biopics dataset"""
    # Conditions for categorizing the subjects
    conditions = [
        (col("subject_race") == "Caucasian") | (col("subject_race") == "White"),
        (col("subject_race") == "African American") | (col("subject_race") == "Black"),
        (col("subject_sex") == "Female"),
    ]

    # Categories to assign based on conditions
    categories = ["White", "Black", "Female"]

    # Apply transformation to categorize subjects based on race and sex
    df = df.withColumn(
        "Subject_Category",
        when(conditions[0], categories[0])
        .when(conditions[1], categories[1])
        .when(conditions[2], categories[2])
        .otherwise("Other"),
    )

    log_output("transform data", df.limit(10).toPandas().to_markdown())

    return df.show()
