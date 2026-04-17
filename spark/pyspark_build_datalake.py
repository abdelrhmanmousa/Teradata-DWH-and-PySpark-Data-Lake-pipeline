from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("DataLake_Banking")
    .enableHiveSupport()
    .getOrCreate()
)

jdbc_url = "jdbc:oracle:thin:@//source-db:1521/BANKDB"
jdbc_props = {
    "user": "etl_user",
    "password": "****",
    "driver": "oracle.jdbc.OracleDriver",
}

RAW_BASE = "hdfs:///datalake/raw"
CURATED_BASE = "hdfs:///datalake/curated"
GOLD_BASE = "hdfs:///datalake/gold"
OPEN_END_DATE = "9999-12-31"


def read_large(table, partition_col, lower, upper, parts=20):
    return spark.read.jdbc(
        url=jdbc_url,
        table=table,
        column=partition_col,
        lowerBound=lower,
        upperBound=upper,
        numPartitions=parts,
        properties=jdbc_props,
    )


def read_small(table):
    return spark.read.jdbc(url=jdbc_url, table=table, properties=jdbc_props)


def normalize_date(df, col_name):
    return df.withColumn(col_name, F.to_date(F.col(col_name)))


def build_scd2(df, business_key_cols, order_col, tracked_cols):
    order_window = Window.partitionBy(*business_key_cols).orderBy(order_col)
    compare_window = Window.partitionBy(*business_key_cols).orderBy(order_col)

    change_condition = F.lit(False)
    for col_name in tracked_cols:
        current_value = F.coalesce(F.col(col_name), F.lit("__NULL__"))
        previous_value = F.coalesce(F.lag(F.col(col_name)).over(compare_window), F.lit("__NULL__"))
        change_condition = (
            change_condition
            | (current_value != previous_value)
        )

    scd_df = (
        df.withColumn("prev_exists", F.lag(F.col(order_col)).over(compare_window).isNotNull())
        .withColumn("is_change", F.when(F.col("prev_exists"), change_condition).otherwise(F.lit(True)))
        .filter(F.col("is_change"))
        .withColumn("start_date", F.col(order_col))
        .withColumn("next_start_date", F.lead(F.col(order_col)).over(order_window))
        .withColumn(
            "end_date",
            F.coalesce(
                F.date_sub(F.col("next_start_date"), 1),
                F.lit(OPEN_END_DATE).cast("date"),
            ),
        )
        .withColumn(
            "is_current",
            F.when(F.col("next_start_date").isNull(), F.lit(1)).otherwise(F.lit(0)),
        )
        .drop("prev_exists", "is_change", "next_start_date")
    )

    return scd_df


# ============================================
# ZONE 1 — RAW
# ============================================
accounts = read_large("ACCOUNTS", "ACC_NO", 1, 999999, 20)
acc_details = read_large("ACCOUNT_DETAILS", "ACC_NO", 1, 999999, 20)
person = read_small("PERSON")
person_profile = read_small("PERSON_PROFILE")
person_iden = read_small("PERSON_IDEN")

accounts.write.mode("overwrite").partitionBy("DATE").orc(f"{RAW_BASE}/accounts/")
acc_details.write.mode("overwrite").partitionBy("DATE").orc(f"{RAW_BASE}/account_details/")
person.write.mode("overwrite").orc(f"{RAW_BASE}/person/")
person_profile.write.mode("overwrite").partitionBy("DATE").orc(f"{RAW_BASE}/person_profile/")
person_iden.write.mode("overwrite").partitionBy("DATE").orc(f"{RAW_BASE}/person_iden/")

print("Zone 1 RAW complete")


# ============================================
# ZONE 2 — CURATED
# ============================================
acc_clean = (
    normalize_date(accounts, "DATE")
    .withColumnRenamed("ACC_NO", "acc_no")
    .withColumnRenamed("DATE", "eff_date")
    .withColumnRenamed("STATUS", "acc_status")
    .withColumn("acc_status", F.trim(F.col("acc_status")))
    .dropDuplicates(["acc_no", "eff_date", "acc_status"])
)

det_clean = (
    normalize_date(acc_details, "DATE")
    .withColumnRenamed("ACC_NO", "acc_no")
    .withColumnRenamed("DATE", "eff_date")
    .withColumnRenamed("TYPE", "acc_type")
    .withColumn("acc_type", F.trim(F.col("acc_type")))
    .dropDuplicates(["acc_no", "eff_date", "acc_type"])
)

bridge_clean = (
    person.withColumnRenamed("ACC_NO", "acc_no")
    .withColumnRenamed("PERSON", "person_id")
    .dropDuplicates(["person_id", "acc_no"])
)

prof_clean = (
    normalize_date(person_profile, "DATE")
    .withColumnRenamed("PERSON", "person_id")
    .withColumnRenamed("NAME", "person_name")
    .withColumnRenamed("DATE", "eff_date")
    .withColumn("person_name", F.trim(F.col("person_name")))
    .dropDuplicates(["person_id", "eff_date", "person_name"])
)

iden_clean = (
    normalize_date(person_iden, "DATE")
    .withColumnRenamed("PERSON", "person_id")
    .withColumnRenamed("DATE", "eff_date")
    .withColumn(
        "id_value",
        F.regexp_replace(F.col("ID"), r"\s*\(.*\)", ""),
    )
    .withColumn(
        "id_type",
        F.when(F.upper(F.col("ID")).contains("NID"), F.lit("NID"))
        .when(F.upper(F.col("ID")).contains("PASS"), F.lit("PASSPORT"))
        .otherwise(F.lit("OTHER")),
    )
    .drop("ID")
    .dropDuplicates(["person_id", "eff_date", "id_value"])
)

acc_clean.write.mode("overwrite").partitionBy("eff_date").parquet(f"{CURATED_BASE}/accounts/")
det_clean.write.mode("overwrite").partitionBy("eff_date").parquet(f"{CURATED_BASE}/account_details/")
bridge_clean.write.mode("overwrite").parquet(f"{CURATED_BASE}/person/")
prof_clean.write.mode("overwrite").partitionBy("eff_date").parquet(f"{CURATED_BASE}/person_profile/")
iden_clean.write.mode("overwrite").partitionBy("eff_date").parquet(f"{CURATED_BASE}/person_iden/")

print("Zone 2 CURATED complete")


# ============================================
# ZONE 3 — GOLD
# ============================================
person_versions = build_scd2(
    prof_clean.select("person_id", "person_name", "eff_date"),
    business_key_cols=["person_id"],
    order_col="eff_date",
    tracked_cols=["person_name"],
)

person_key_window = Window.orderBy("person_id", "start_date")
dim_person = (
    person_versions.withColumn("person_sk", F.row_number().over(person_key_window))
    .select(
        "person_sk",
        "person_id",
        "person_name",
        "start_date",
        "end_date",
        "is_current",
    )
)

account_source = (
    acc_clean.join(det_clean, on=["acc_no", "eff_date"], how="inner")
    .select("acc_no", "eff_date", "acc_status", "acc_type")
)

account_versions = build_scd2(
    account_source,
    business_key_cols=["acc_no"],
    order_col="eff_date",
    tracked_cols=["acc_status", "acc_type"],
)

account_key_window = Window.orderBy("acc_no", "start_date")
dim_account = (
    account_versions.withColumn("account_sk", F.row_number().over(account_key_window))
    .select(
        "account_sk",
        "acc_no",
        "acc_status",
        "acc_type",
        "start_date",
        "end_date",
        "is_current",
    )
)

dim_person_iden = (
    iden_clean.alias("i")
    .join(
        dim_person.alias("p"),
        (
            (F.col("i.person_id") == F.col("p.person_id"))
            & (F.col("i.eff_date") >= F.col("p.start_date"))
            & (F.col("i.eff_date") <= F.col("p.end_date"))
        ),
        "inner",
    )
    .select(
        F.row_number().over(Window.orderBy("p.person_sk", "i.eff_date", "i.id_value")).alias("iden_sk"),
        F.col("p.person_sk").alias("person_sk"),
        F.col("i.id_value").alias("id_value"),
        F.col("i.id_type").alias("id_type"),
        F.col("i.eff_date").alias("start_date"),
    )
)

bridge_person_account = (
    bridge_clean.alias("b")
    .join(
        dim_person.filter(F.col("is_current") == 1).alias("p"),
        F.col("b.person_id") == F.col("p.person_id"),
        "inner",
    )
    .join(
        dim_account.filter(F.col("is_current") == 1).alias("a"),
        F.col("b.acc_no") == F.col("a.acc_no"),
        "inner",
    )
    .select(
        F.col("p.person_sk").alias("person_sk"),
        F.col("a.account_sk").alias("account_sk"),
    )
    .dropDuplicates(["person_sk", "account_sk"])
)

snapshot_date = F.current_date()
fact_account_snapshot = (
    bridge_person_account.alias("b")
    .join(dim_person.alias("p"), F.col("b.person_sk") == F.col("p.person_sk"), "inner")
    .join(dim_account.alias("a"), F.col("b.account_sk") == F.col("a.account_sk"), "inner")
    .where(
        (snapshot_date >= F.col("p.start_date"))
        & (snapshot_date <= F.col("p.end_date"))
        & (snapshot_date >= F.col("a.start_date"))
        & (snapshot_date <= F.col("a.end_date"))
    )
    .select(
        snapshot_date.alias("snapshot_date"),
        F.date_format(snapshot_date, "yyyyMMdd").cast("int").alias("date_sk"),
        F.col("b.person_sk").alias("person_sk"),
        F.col("b.account_sk").alias("account_sk"),
        F.when(F.upper(F.col("a.acc_status")) == "ACTIVE", F.lit(1)).otherwise(F.lit(0)).alias("status_flag"),
    )
)

dim_person.write.mode("overwrite").option("path", f"{GOLD_BASE}/dim_person").saveAsTable("gold.dim_person")
dim_account.write.mode("overwrite").option("path", f"{GOLD_BASE}/dim_account").saveAsTable("gold.dim_account")
dim_person_iden.write.mode("overwrite").option("path", f"{GOLD_BASE}/dim_person_iden").saveAsTable("gold.dim_person_iden")
bridge_person_account.write.mode("overwrite").option("path", f"{GOLD_BASE}/bridge_person_account").saveAsTable("gold.bridge_person_account")
fact_account_snapshot.write.mode("overwrite").partitionBy("snapshot_date").option("path", f"{GOLD_BASE}/fact_account_snapshot").saveAsTable("gold.fact_account_snapshot")

print("Zone 3 GOLD complete — Data Lake ready")
