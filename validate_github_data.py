import yaml
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder.appName("GitHubDataValidation").getOrCreate()

# Load data from GitHub
github_data_url = "https://raw.githubusercontent.com/your-username/your-repo/main/data.csv"  # Replace with actual URL
data_file = "/tmp/github_data.csv"
with open(data_file, "wb") as f:
    f.write(requests.get(github_data_url).content)

df = spark.read.option("header", "true").csv(data_file)

# Load validation rules from YAML
yaml_path = "validation_rules.yaml"  # Ensure this file is accessible
with open(yaml_path, "r") as f:
    rules = yaml.safe_load(f)

# Apply schema validation
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

df = apply_schema(df, rules)

# Validation report
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
        regex_check = df.filter(~col(col_name).rlike(regex)).count() == 0
        validation_results.append((col_name, f"Regex match: {regex}", regex_check))

    if "format" in constraints and col_rule["type"] == "date":
        # Date format validation can be added here if needed
        pass

# Display validation results
print("\nValidation Report:")
for col_name, rule, passed in validation_results:
    status = "✅ Passed" if passed else "❌ Failed"
    print(f"{col_name} - {rule}: {status}")
