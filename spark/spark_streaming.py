from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import concat_ws

# ------------------------------------------------------------------------------
# 1. Spark Session
# ------------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("JobAnalyticsKafkaToStarSchema") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------------------------------------
# 2. Read from Kafka
# ------------------------------------------------------------------------------
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "job_applications") \
    .option("startingOffsets", "latest") \
    .load()

# ------------------------------------------------------------------------------
# 3. Define Kafka Event Schema (FINAL)
# ------------------------------------------------------------------------------
schema = StructType([
    StructField("application", StructType([
        StructField("application_id", IntegerType()),
        StructField("application_time", StringType())
    ])),
    StructField("job", StructType([
        StructField("job_id", IntegerType()),
        StructField("job_title", StringType()),
        StructField("role", StringType()),
        StructField("role_level", StringType()),
        StructField("domain", StringType()),
        StructField("industry", StringType()),
        StructField("company_id", IntegerType()),
        StructField("company_name", StringType()),
        StructField("job_location", StringType())
    ])),
    StructField("applicant", StructType([
        StructField("applicant_id", IntegerType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("email", StringType()),
        StructField("experience_years", IntegerType()),
        StructField("experience_level", StringType()),
        StructField("visa_status", StringType()),
        StructField("region", StringType()),
        StructField("state", StringType()),
        StructField("state_abbr", StringType())
    ]))
])

# ------------------------------------------------------------------------------
# 4. Parse Kafka JSON
# ------------------------------------------------------------------------------
parsed_df = raw_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# ------------------------------------------------------------------------------
# 5. DIMENSION TABLES
# ------------------------------------------------------------------------------

# ------------------ dim_company ------------------
dim_company = parsed_df.select(
    col("job.company_id").alias("company_id"),
    col("job.company_name").alias("company_name")
).dropDuplicates(["company_id"])

# ------------------ dim_job ------------------
dim_job = parsed_df.select(
    col("job.job_id").alias("job_id"),
    col("job.job_title").alias("job_title"),
    col("job.role").alias("role"),
    col("job.role_level").alias("role_level"),
    col("job.domain").alias("domain"),
    col("job.industry").alias("industry"),
    col("job.job_location").alias("job_location"),
    col("job.company_id").alias("company_id")
).dropDuplicates(["job_id"])

# ------------------ dim_applicant ------------------
dim_applicant = parsed_df.select(
    col("applicant.applicant_id").alias("applicant_id"),
    concat_ws(" ", col("applicant.first_name"), col("applicant.last_name")).alias("applicant_name"),
    col("applicant.email").alias("email"),
    col("applicant.experience_years").alias("experience_years"),
    col("applicant.experience_level").alias("experience_level"),
    col("applicant.visa_status").alias("visa_status"),
    col("applicant.state").alias("state"),
    col("applicant.state_abbr").alias("state_abbr")
).dropDuplicates(["applicant_id"])

# ------------------ dim_region (US States) ------------------
dim_region = parsed_df.select(
    col("applicant.state").alias("state"),
    col("applicant.state_abbr").alias("state_abbr")
).dropDuplicates()

# ------------------------------------------------------------------------------
# 6. FACT TABLE
# ------------------------------------------------------------------------------
fact_job_applications = parsed_df.select(
    col("application.application_id").alias("application_id"),
    to_timestamp(col("application.application_time")).alias("applied_timestamp"),
    col("job.job_id").alias("job_id"),
    col("job.company_id").alias("company_id"),
    col("applicant.applicant_id").alias("applicant_id"),
    col("applicant.state_abbr").alias("region"),
    col("applicant.experience_years").alias("experience_years"),
    col("applicant.visa_status").alias("visa_status")
)


# ------------------------------------------------------------------------------
# 7. WRITE STREAMS (PARQUET)
# ------------------------------------------------------------------------------

# ---- Dimensions ----
dim_company.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "output/dim_company") \
    .option("checkpointLocation", "output/checkpoints/dim_company") \
    .start()

dim_job.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "output/dim_job") \
    .option("checkpointLocation", "output/checkpoints/dim_job") \
    .start()

dim_applicant.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "output/dim_applicant") \
    .option("checkpointLocation", "output/checkpoints/dim_applicant") \
    .start()

dim_region.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "output/dim_region") \
    .option("checkpointLocation", "output/checkpoints/dim_region") \
    .start()

# ---- Fact ----
fact_job_applications.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "output/fact_job_applications") \
    .option("checkpointLocation", "output/checkpoints/fact_job_applications") \
    .start()

# ------------------------------------------------------------------------------
# 8. Await Termination
# ------------------------------------------------------------------------------
spark.streams.awaitAnyTermination()
