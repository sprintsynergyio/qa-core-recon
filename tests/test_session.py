from pathlib import Path

import pytest
from py4j.protocol import Py4JJavaError

from qa_core_recon.spark import (
    ICEBERG_SPARK_PACKAGE,
    MAVEN_CENTRAL,
    catalog_name_for,
    ivy_cache_dir,
    local_session,
)


def test_session_is_local_and_fixed(spark):
    spark.range(1).count()
    assert spark.sparkContext.master == "local[1]"
    assert spark.conf.get("spark.sql.shuffle.partitions") == "1"
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    assert spark.conf.get("spark.ui.enabled") == "false"
    assert spark.conf.get("spark.sql.catalogImplementation") == "in-memory"
    assert spark.conf.get("spark.jars.packages") == ICEBERG_SPARK_PACKAGE
    assert spark.conf.get("spark.jars.ivy") == str(ivy_cache_dir())
    assert spark.conf.get("spark.jars.repositories") == MAVEN_CENTRAL
    pending = catalog_name_for("/not/a/registered/warehouse")
    with pytest.raises(Py4JJavaError):
        spark.conf.get(f"spark.sql.catalog.{pending}")
    assert spark.conf.get("spark.driver.host") == "127.0.0.1"
    assert spark.conf.get("spark.driver.bindAddress") == "127.0.0.1"
    assert spark.conf.get("spark.sql.caseSensitive") == "true"
    assert spark.conf.get("spark.sql.execution.arrow.pyspark.enabled") == "false"
    assert spark.conf.get("spark.sql.csv.parser.columnPruning.enabled") == "false"
    assert local_session() is spark


def test_session_leaves_no_hive_files_in_the_repo(spark):
    spark.range(1).count()
    root = Path(__file__).resolve().parents[1]
    for name in ("metastore_db", "derby.log", "spark-warehouse"):
        assert not (root / name).exists()
        assert not (Path.cwd() / name).exists()
    warehouse = spark.conf.get("spark.sql.warehouse.dir")
    assert str(root) not in warehouse
    ivy = Path(spark.conf.get("spark.jars.ivy"))
    assert root not in ivy.parents
    assert Path.cwd() not in ivy.parents
