age_datafrome_rows = [("Alice", 34), ("Bob", 45), ("Catherine", 29)]
age_dataframe_columns = ["Name", "Age"]

class_dataframe_rows = [("Alice", "Math"), ("Bob", "Science"), ("Catherine", "History")]
class_dataframe_columns = ["Name", "Class"]


def create_class_dataframe(spark):
  return spark.createDataFrame(class_dataframe_rows, class_dataframe_columns)
  
def create_age_dataframe(spark):
  return spark.createDataFrame(age_datafrome_rows, age_dataframe_columns)