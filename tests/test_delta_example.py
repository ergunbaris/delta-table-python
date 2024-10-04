import pytest
from pyspark.sql import SparkSession
from delta import *
from src.delta_example.main import create_spark_session, run_delta_example, query_delta_table, drop_delta_table


@pytest.fixture(scope="module")
def spark():
    return create_spark_session()


def test_run_delta_example(spark):
    tableName = str("default.test_delta_table")
    run_delta_example(spark, tableName)
    result = query_delta_table(spark, tableName, 0)
    # Check if the result has the expected schema
    assert result.columns == ['id', 'value']
    # Check if the result has the expected number of rows
    assert result.count() == 3

    result = query_delta_table(spark, tableName, 1)
    # Check if the result has the expected schema
    assert result.columns == ['id', 'value']
    # Check if the result has the expected number of rows
    assert result.count() == 3
    # Check if the update operation was successful
    updated_row = result.filter("id = 2").collect()[0]
    assert updated_row['value'] == 'B_updated'


    result = query_delta_table(spark, tableName, 2)
    # Check if the result has the expected schema
    assert result.columns == ['id', 'value']
    # Check if the result has the expected number of rows
    assert result.count() == 2

    drop_delta_table(spark, tableName)


