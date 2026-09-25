import sys
import logging
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, LongType

sys.path.append('/Workspace/Users/iamhadiya13@gmail.com/Supply Chain/Ingestion/transformations')
sys.path.append('/Workspace/Users/iamhadiya13@gmail.com/Supply Chain/config')

from cleaning_silver_gold import validates_column, audit_columns
from config_gold import GOLD_CONFIG, SILVER_PATH

GOLD_SCHEMA = "supply_chain.gold"
CONTROL_TBL = "supply_chain.control.pipeline_control_gold"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ── watermark read ───────────────────────────────────────────────────
def get_watermark(spark, table_name):
    try:
        row = (spark.table(CONTROL_TBL)
               .filter(
                   (F.col("table_name") == table_name) &
                   (F.col("status") == "SUCCESS")
               )
               .orderBy(F.col("last_run_time").desc())
               .first())
        return row["last_file_date"] if row else None
    except Exception:
        return None


# ── watermark save ───────────────────────────────────────────────────
def save_watermark(spark, table_name, last_file_date, rows, status, error=None):
    data = [(table_name, last_file_date, datetime.now(), rows, status, error)]
    schema = StructType([
        StructField("table_name", StringType(), True),
        StructField("last_file_date", DateType(), True),
        StructField("last_run_time", TimestampType(), True),
        StructField("rows_processed", LongType(), True),
        StructField("status", StringType(), True),
        StructField("error_message", StringType(), True),
    ])

    (spark.createDataFrame(data, schema)
     .write.format("delta")
     .mode("append")
     .saveAsTable(CONTROL_TBL))


# ── read silver (incremental or full) ───────────────────────────────
def read_silver(spark, path, table_name, load_type):
    last_date = get_watermark(spark, table_name)

    if load_type == "FULL" or last_date is None:
        logger.info(f"{table_name}: FULL load")
        df = spark.read.format("delta").load(path)
        new_max = df.agg(F.max("file_date")).collect()[0][0]
        return df, new_max

    df = (spark.read.format("delta").load(path)
          .filter(F.col("file_date") >= F.lit(last_date)))

    new_max = df.agg(F.max("file_date")).collect()[0][0]

    if new_max is None:
        logger.info(f"{table_name}: no new data since {last_date}")
        return None, last_date

    logger.info(f"{table_name}: reading file_date > {last_date}, max = {new_max}")
    return df, new_max


# ── merge into gold ──────────────────────────────────────────────────
def merge_gold_table(spark, df, table_name, key_columns, gold_columns=None):
    if isinstance(key_columns, str):
        key_columns = [key_columns]
    target_table = f"{GOLD_SCHEMA}.{table_name}"

    # Deduplicate source rows by key to prevent DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE
    order_cols = [F.col("file_date").desc()]
    if "created_at" in df.columns:
        order_cols.append(F.col("created_at").desc())
    if "last_replenishment_date" in df.columns:
        order_cols.append(F.col("last_replenishment_date").desc())
    w = Window.partitionBy(*key_columns).orderBy(*order_cols)
    df = (df.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn"))

    if gold_columns:
        df = df.select(gold_columns)

    df = audit_columns(df)

    if spark.catalog.tableExists(target_table):
        target    = DeltaTable.forName(spark, target_table)
        condition = " AND ".join(f"target.{k} = source.{k}" for k in key_columns)

        update_col = [c for c in df.columns if c != "Insert_dt"]
        update_set = {c: f"source.{c}" for c in update_col}
        update_condition = " OR ".join(f"NOT (target.{c} <=> source.{c})" for c in update_col)

        (target.alias("target")
               .merge(df.alias("source"), condition)
               .whenMatchedUpdate(set=update_set, condition=update_condition)
               .whenNotMatchedInsertAll()
               .execute())

        logger.info(f"Merge completed for {table_name}")
    else:
        df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        logger.info(f"Table {target_table} created")


# ── process each table ───────────────────────────────────────────────
def process_table(spark, table_name, cfg):
    source     = cfg["source"]
    path       = SILVER_PATH[source]
    load_type  = cfg.get("load_type", "INCREMENTAL")
    key        = cfg["key"]
    columns    = cfg["columns"]

    try:
        df, new_max = read_silver(spark, path, table_name, load_type)

        if df is None:
            save_watermark(spark, table_name, new_max, 0, "SKIPPED")
            return "SKIPPED"

        validates_column(df, columns, table_name)

        row_count = df.count()
        merge_gold_table(spark, df, table_name, key, columns)
        save_watermark(spark, table_name, new_max, row_count, "SUCCESS")

        logger.info(f"{table_name}: merged {row_count} rows")
        return "SUCCESS"

    except Exception as e:
        save_watermark(spark, table_name, None, 0, "FAILED", str(e))
        logger.exception(f"{table_name}: FAILED — {e}")
        return "FAILED"


# ── main ─────────────────────────────────────────────────────────────
def main():
    spark = SparkSession.builder.appName("silver_to_gold").getOrCreate()
    logger.info("Starting Silver to Gold")

    results = {}
    for table_name, cfg in GOLD_CONFIG.items():
        if not cfg.get("is_active", True):
            logger.info(f"{table_name}: skipped (inactive)")
            results[table_name] = "SKIPPED"
            continue
        results[table_name] = process_table(spark, table_name, cfg)

    failed  = [t for t, s in results.items() if s == "FAILED"]
    skipped = [t for t, s in results.items() if s == "SKIPPED"]
    success = len(results) - len(failed) - len(skipped)

    logger.info(f"Done. success={success} skipped={len(skipped)} failed={len(failed)}")

    if failed:
        raise RuntimeError(f"Failed tables: {', '.join(failed)}")


if __name__ == "__main__":
    main()