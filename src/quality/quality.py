import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from soda.scan import Scan

__all__ = ["verify_quality"]

logger = logging.getLogger(__name__)


def verify_quality(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    contract_path: str | Path,
) -> None:
    """Run Soda data quality checks on a DataFrame.

    Registers the DataFrame as a temp view named ``table_name`` and runs the
    SodaCL checks declared in ``contract_path`` against it. Raises ``ValueError``
    if any check fails so upstream callers (e.g. Airflow tasks) can mark the
    run as failed.
    """
    df.createOrReplaceTempView(table_name)

    scan = Scan()
    scan.set_data_source_name("spark_df")
    scan.add_spark_session(spark)
    scan.set_scan_definition_name(f"{table_name}_quality")
    scan.add_sodacl_yaml_file(str(contract_path))
    scan.execute()

    if scan.has_check_fails():
        raise ValueError(
            f"Soda quality checks failed for {table_name}:\n{scan.get_logs_text()}"
        )

    logger.info("All quality checks passed for %s", table_name)
