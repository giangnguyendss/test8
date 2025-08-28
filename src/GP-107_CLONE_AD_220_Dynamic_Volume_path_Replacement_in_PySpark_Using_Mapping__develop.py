spark.catalog.setCurrentCatalog("purgo_databricks")

# -----------------------------------------------------------------------------------
# PySpark Script for Clinical Trial Data Processing in Databricks
# -----------------------------------------------------------------------------------
# Catalog: purgo_databricks
# Schema: purgo_playground
# Volume paths updated as per volume_mapping_sheet.xlsx
# Only volume paths are changed; all other logic remains unchanged
# -----------------------------------------------------------------------------------

# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql.functions import col, avg, countDistinct  

# Read CSV Files with updated volume paths from mapping sheet
biomarker_df = spark.read.option("header", True).csv("/Volumes/agilisium_playground/purgo_playground/de_dq/clinical_trail/19_03_2025_Biomarker.csv")
patient_df = spark.read.option("header", True).csv("/Volumes/agilisium_playground/purgo_playground/de_dq/clinical_trail/19_03_2025_Patient_Demographics.csv")
site_df = spark.read.option("header", True).csv("/Volumes/agilisium_playground/purgo_playground/de_dq/clinical_trail/19_03_2025_Site_Data.csv")

# Data Cleaning: Handling Missing Values (Fill NaN with Mean)
biomarker_df = biomarker_df.fillna({
    "Biomarker_A (mg/dL)": biomarker_df.select(avg(col("Biomarker_A (mg/dL)"))).collect()[0][0],
    "Biomarker_B (ng/mL)": biomarker_df.select(avg(col("Biomarker_B (ng/mL)"))).collect()[0][0],
    "Biomarker_C (pg/mL)": biomarker_df.select(avg(col("Biomarker_C (pg/mL)"))).collect()[0][0]
})

# Count Distinct Patients
distinct_patient_count = patient_df.select(countDistinct(col("Patient_ID")).alias("Unique_Patients"))
display(distinct_patient_count)

# Compute Average Biomarker Levels Per Patient
biomarker_avg_df = biomarker_df.groupBy("Patient_ID").agg(
    avg(col("Biomarker_A (mg/dL)")).alias("Avg_Biomarker_A"),
    avg(col("Biomarker_B (ng/mL)")).alias("Avg_Biomarker_B"),
    avg(col("Biomarker_C (pg/mL)")).alias("Avg_Biomarker_C")
)
display(biomarker_avg_df)

# Join Biomarker Data with Patient Demographics
patient_biomarker_df = biomarker_avg_df.join(patient_df, "Patient_ID", "left")

# Join with Site Details
final_df = patient_biomarker_df.join(biomarker_df.select("Patient_ID", "Site"), "Patient_ID", "left") \
    .join(site_df, "Site", "left") \
    .select("Patient_ID", "Age", "Gender", "Weight (kg)", "Location", "Site_Type",
            "Avg_Biomarker_A", "Avg_Biomarker_B", "Avg_Biomarker_C")

# Show Final Processed Data
display(final_df)
# -----------------------------------------------------------------------------------
# End of Script
# -----------------------------------------------------------------------------------
