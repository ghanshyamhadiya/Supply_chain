import logging

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def validates_column(df: DataFrame, required_columns, table_name):
    missing_column = [
        column_name for column_name in required_columns
        if column_name not in df.columns
    ]

    if missing_column:
        raise ValueError(
            f"Missing column: {missing_column} in table: {table_name}"
        )


def audit_columns(df: DataFrame):
    current_ts = F.current_timestamp()

    return (
        df.withColumn("Insert_dt", current_ts)
        .withColumn("Update_dt", current_ts)
    )