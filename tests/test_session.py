from pathlib import Path

from qa_core_recon.spark import local_session


def test_session_is_local_and_fixed(spark):
    spark.range(1).count()
    assert spark.sparkContext.master == "local[1]"
    assert spark.conf.get("spark.sql.shuffle.partitions") == "1"
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    assert spark.conf.get("spark.ui.enabled") == "false"
    assert spark.conf.get("spark.sql.catalogImplementation") == "in-memory"
    assert spark.conf.get("spark.driver.host") == "127.0.0.1"
    assert spark.conf.get("spark.driver.bindAddress") == "127.0.0.1"
    assert spark.conf.get("spark.sql.caseSensitive") == "true"
    assert spark.conf.get("spark.sql.execution.arrow.pyspark.enabled") == "false"
    assert local_session() is spark


def test_session_leaves_no_hive_files_in_the_repo(spark):
    spark.range(1).count()
    root = Path(__file__).resolve().parents[1]
    for name in ("metastore_db", "derby.log", "spark-warehouse"):
        assert not (root / name).exists()
        assert not (Path.cwd() / name).exists()
    warehouse = spark.conf.get("spark.sql.warehouse.dir")
    assert str(root) not in warehouse
