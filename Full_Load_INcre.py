# Databricks notebook source
# MAGIC %sql
# MAGIC -- RUN this SQL cell first
# MAGIC CREATE CATALOG IF NOT EXISTS hsbc;
# MAGIC CREATE SCHEMA IF NOT EXISTS hsbc.hbsc_tables;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS hsbc.hbsc_tables.control_table_ops (
# MAGIC   table_name STRING,
# MAGIC   load_date STRING,
# MAGIC   operation STRING,
# MAGIC   incoming_count LONG,
# MAGIC   inserted LONG,
# MAGIC   updated LONG,
# MAGIC   delta_version LONG,
# MAGIC   created_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC

# COMMAND ----------

# RUN this Python cell
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from delta.tables import DeltaTable
from datetime import datetime, timedelta
import random, uuid

spark = SparkSession.builder.getOrCreate()

# ----- PARAMETERS (change agar chahiye) -----
DATE_STR = "2025-11-10"              # jo date label chahiye
TABLES = ["table1"]                  # agar 90 tables chahiye to list extend karo
FULL_COUNT = 100
INC_OVERLAP = 10
INC_NEW = 40

# ----- helper to make rows -----
first_names = ["Mohit","Rahul","Amit","Anita","Priya","Suman","Ravi","Neha","Deepak","Pooja"]
last_names  = ["Sharma","Singh","Verma","Kumar","Gupta","Jain","Agarwal"]

def make_row_dict(pk_index):
    pk = str(uuid.uuid4())
    return {
        "id": pk,
        "name": random.choice(first_names) + " " + random.choice(last_names),
        "amount": random.randint(100,20000),
        "status": random.choice(["NEW","OPEN","CLOSED"]),
        "event_date": (datetime.now() - timedelta(days=random.randint(0,30))).isoformat()
    }

# ----- function to build mock dataframes in-memory per table -----
def generate_mock_for_table(table_name):
    # FULL rows
    full_rows = []
    for i in range(FULL_COUNT):
        r = make_row_dict(i)
        full_rows.append(r)
    # Keep first INC_OVERLAP ids for overlap
    overlap_ids = [full_rows[i]["id"] for i in range(INC_OVERLAP)]

    # INCREMENTAL rows: modified overlaps + new
    inc_rows = []
    for i, pk in enumerate(overlap_ids):
        # modify some fields to simulate update
        rec = {
            "id": pk,
            "name": random.choice(first_names) + " " + random.choice(last_names),
            "amount": full_rows[i]["amount"] + 500,   # changed amount
            "status": random.choice(["OPEN","CLOSED"]),
            "event_date": full_rows[i]["event_date"]
        }
        inc_rows.append(rec)
    for i in range(INC_NEW):
        rec = make_row_dict(FULL_COUNT + i)
        inc_rows.append(rec)

    # Convert to DataFrame
    full_df = spark.createDataFrame(full_rows)
    inc_df = spark.createDataFrame(inc_rows)
    return full_df, inc_df

# QUICK TEST generation for first table
test_full, test_inc = generate_mock_for_table("table1")
print("Generated FULL rows:", test_full.count(), "INCR rows:", test_inc.count())


# COMMAND ----------

# RUN this Python cell (contains process function)
from pyspark.sql.functions import col

def write_full_as_managed_table(catalog, schema, table_name, full_df, date_str):
    qualified = f"{catalog}.{schema}.{table_name}"
    # add metadata
    df = full_df.withColumn("_load_date", lit(date_str)).withColumn("_operation", lit("FULL_LOAD")) \
                .withColumn("_ingested_at", current_timestamp())
    # write as managed Delta table in Unity Catalog
    # This will create a managed Delta table under Unity Catalog (no /mnt paths)
    df.write.format("delta").mode("overwrite").saveAsTable(qualified)
    return df.count()

def merge_incremental_into_table(catalog, schema, table_name, inc_df, date_str):
    qualified = f"{catalog}.{schema}.{table_name}"
    # ensure table exists
    try:
        DeltaTable.forName(spark, qualified)
    except Exception as e:
        raise RuntimeError(f"Target table {qualified} does not exist. Run FULL first.") from e

    # compute exact inserted / updated counts BEFORE merge
    existing_ids_df = spark.table(qualified).select("id").distinct()
    incoming_ids_df = inc_df.select("id").distinct()

    inserted_count = incoming_ids_df.join(existing_ids_df, on="id", how="left_anti").count()
    updated_count  = incoming_ids_df.join(existing_ids_df, on="id", how="inner").count()
    total_incoming = inc_df.count()

    # prepare inc_df with metadata and temp view
    inc_prepped = inc_df.withColumn("_load_date", lit(date_str)).withColumn("_operation", lit("INCREMENTAL_LOAD")) \
                        .withColumn("_ingested_at", current_timestamp())
    inc_prepped.createOrReplaceTempView("incoming_inc_tmp")

    # MERGE using catalog-qualified name
    merge_sql = f"""
    MERGE INTO {qualified} t
    USING incoming_inc_tmp s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(merge_sql)

    # get delta version after merge
    try:
        ver = spark.sql(f"DESCRIBE HISTORY {qualified}").select("version").orderBy(col("version").desc()).limit(1).collect()[0][0]
    except Exception:
        ver = None

    return total_incoming, inserted_count, updated_count, ver

def record_control(catalog, schema, table_name, date_str, operation, incoming, inserted, updated, version):
    qualified_ctrl = f"{catalog}.{schema}.control_table_ops"
    row = [(f"{catalog}.{schema}.{table_name}", date_str, operation, incoming, inserted, updated, version, datetime.now())]
    ctrl_df = spark.createDataFrame(row, schema=["table_name","load_date","operation","incoming_count","inserted","updated","delta_version","created_at"])
    ctrl_df.write.format("delta").mode("append").saveAsTable(qualified_ctrl)


# COMMAND ----------

# RUN this cell in same notebook/session (Hinglish)
from datetime import datetime
import random, uuid
from pyspark.sql.functions import lit, current_timestamp

catalog = "hsbc"
schema  = "hbsc_tables"
table   = "table1"
qualified = f"{catalog}.{schema}.{table}"
date_str = "2025-11-10"   # same date label

# 1) get 10 existing ids from the table (for overlap)
existing_ids = [row['id'] for row in spark.table(qualified).select("id").limit(10).collect()]
print("Picked overlap ids (count):", len(existing_ids))

# 2) build incremental rows: first modify those 10, then add 40 new
first_names = ["Mohit","Rahul","Amit","Anita","Priya","Suman","Ravi","Neha","Deepak","Pooja"]
last_names  = ["Sharma","Singh","Verma","Kumar","Gupta","Jain","Agarwal"]

inc_rows = []

# overlapping (update) rows - change amount/status to simulate update
for i, pk in enumerate(existing_ids):
    rec = {
        "id": pk,
        "name": random.choice(first_names) + " " + random.choice(last_names),
        "amount": random.randint(5000,25000),   # changed amount
        "status": random.choice(["OPEN","CLOSED"]),
        "event_date": datetime.now().isoformat()
    }
    inc_rows.append(rec)

# 40 new rows (inserts)
for i in range(40):
    pk = str(uuid.uuid4())
    rec = {
        "id": pk,
        "name": random.choice(first_names) + " " + random.choice(last_names),
        "amount": random.randint(100,20000),
        "status": random.choice(["NEW","OPEN","CLOSED"]),
        "event_date": datetime.now().isoformat()
    }
    inc_rows.append(rec)

# create DataFrame
inc_df = spark.createDataFrame(inc_rows)
print("Incremental DF rows:", inc_df.count())

# 3) Merge into Unity Catalog table (uses earlier defined function merge_incremental_into_table)
# If you haven't run the function cell that defines merge_incremental_into_table and record_control,
# run that cell first. Otherwise this will call it directly.

tot_inc, ins_cnt, upd_cnt, ver = merge_incremental_into_table(catalog, schema, table, inc_df, date_str)
print(f"MERGE completed -> incoming={tot_inc}, inserted={ins_cnt}, updated={upd_cnt}, version={ver}")

# 4) record into control table
record_control(catalog, schema, table, date_str, "INCREMENTAL_LOAD", tot_inc, ins_cnt, upd_cnt, ver)
print("Control table updated.")

# 5) verify final counts
final_count = spark.table(qualified).count()
print(f"Final count in {qualified}: {final_count}")

# show last 5 control rows
spark.table(f"{catalog}.{schema}.control_table_ops").orderBy("created_at", ascending=False).show(5, truncate=False)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hsbc.hbsc_tables.control_table_ops ORDER BY created_at DESC;
# MAGIC

# COMMAND ----------

spark.table("hsbc.hbsc_tables.control_table_ops").display()


# COMMAND ----------

spark.table("hsbc.hbsc_tables.table1").display()   #