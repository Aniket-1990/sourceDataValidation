"""
Environment: 
    Python 3.11.9, pip 24.0
    PySpark 3.5.1 (Scala 2.12.18, OpenJDK 17.0.10)

To install packages, use `pip install <package_name>` in the shell.
To save dependencies for future use, run `pip freeze > requirements.txt`

Modify the `Run` command by changing `execute.Run` in `ci-config.json`.
Additional commands can be added in `ci-config.json`.

"""
import yaml
import requests
from pyspark.sql import SparkSession
from dataset_utils import create_class_dataframe, create_age_dataframe
from pyspark.sql.functions import col, regexp_extract,to_date
from pyspark.sql.types import *

def apply_schema(df, rules):
    schema = []
    for col_rule in rules["columns"]:
        col_name = col_rule["name"]
        col_type = col_rule["type"]
        nullable = not col_rule.get("required", False)

        spark_type = {
            "string": StringType(),
            "integer": IntegerType(),
            "float": FloatType(),
            "date": DateType(),
        }.get(col_type, StringType())

        schema.append(StructField(col_name, spark_type, nullable))

    return spark.createDataFrame(df.rdd, StructType(schema))

def main():
  spark = SparkSession.builder \
    .appName("CodeInterview.io") \
    .getOrCreate()
  github_data_url = "https://raw.githubusercontent.com/Aniket-1990/sourceDataValidation/main/test_data.csv"  # Replace with actual URL
  data_file = "/tmp/github_data.csv"
  with open(data_file, "wb") as f:
    f.write(requests.get(github_data_url).content)

  df = spark.read.option("header", "true").csv(data_file)
  yaml_path = "validation_rules.yaml"  # Ensure this file is accessible
  with open(yaml_path, "r") as f:
    rules = yaml.safe_load(f)
  
  df = df.withColumn("id", col("id").cast("int")).withColumn("age", col("age").cast("int")).withColumn("signup_date", to_date("signup_date", "yyyy-MM-dd"))
  
  df = apply_schema(spark,df, rules)
  validation_results = []

  for col_rule in rules["columns"]:
        col_name = col_rule["name"]
        constraints = col_rule.get("constraints", {})
        required = col_rule.get("required", False)

        if required:
            null_count = df.filter(col(col_name).isNull()).count()
            validation_results.append((col_name, "NotNull", null_count == 0))

        if "min" in constraints:
            min_val = constraints["min"]
            min_check = df.filter(col(col_name) < min_val).count() == 0
            validation_results.append((col_name, f"Min >= {min_val}", min_check))

        if "max" in constraints:
            max_val = constraints["max"]
            max_check = df.filter(col(col_name) > max_val).count() == 0
            validation_results.append((col_name, f"Max <= {max_val}", max_check))

        if "regex" in constraints:
            regex = constraints["regex"]
            unmatched_records= df.filter(~col(col_name).rlike(regex))
            
            regex_check= unmatched_records.count() == 0
            if(regex_check==False):
             unmatched_records.show()
            validation_results.append((col_name, f"Regex match: {regex}", regex_check))

  print("\nValidation Report:")
  for col_name, rule, passed in validation_results:
     status = "✅ Passed" if passed else "❌ Failed"
     print(f"{col_name} - {rule}: {status}")     


  spark.stop()

def apply_schema(spark, df, rules):
    schema = []
    for col_rule in rules["columns"]:
        col_name = col_rule["name"]
        col_type = col_rule["type"]
        nullable = not col_rule.get("required", False)

        spark_type = {
            "string": StringType(),
            "integer": IntegerType(),
            "float": FloatType(),
            "date": DateType(),
        }.get(col_type, StringType())

        schema.append(StructField(col_name, spark_type, nullable))

    return spark.createDataFrame(df.rdd, StructType(schema))

if __name__ == '__main__':
  main()
