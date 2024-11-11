import os
import pytest
from mylib.lib import (
    extract,
    load_data,
    describe,
    query,
    example_transform,
    start_spark,
    end_spark,
)


# Pytest fixture for Spark session
@pytest.fixture(scope="module")
def spark():
    spark = start_spark("TestApp")
    yield spark
    end_spark(spark)


def test_extract():
    file_path = extract()
    assert os.path.exists(file_path) is True


def test_load_data(spark):
    df = load_data(spark)
    assert df is not None
    assert df.count() > 0  # Ensure the dataframe is not empty


def test_describe(spark):
    df = load_data(spark)
    result = describe(df)
    # Assert that describe returns a DataFrame with statistics (it shouldn't be None)
    assert result is not None
    assert result.count() > 0  # Ensure some statistics are being returned


def test_query(spark):
    df = load_data(spark)
    result = query(
        spark, df, "SELECT * FROM biopics WHERE number_of_subjects = 4", "biopics"
    )
    # Ensure the result is not None and is a DataFrame
    assert result is not None
    assert result.count() > 0  # Check that the query returns results


def test_example_transform(spark):
    df = load_data(spark)
    result = example_transform(df)
    # Example transform should return a DataFrame, check that it's not empty
    assert result is not None
    assert result.count() > 0  # Ensure the transformation results in some data
