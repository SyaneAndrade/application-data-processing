from pyspark.sql import DataFrame


class DataAcessObject(object):
    """This is a class to acess and write the data object.

    Args:
        spark (SparkSession): The entry point to Spark.
    """

    def __init__(self, spark) -> None:
        self.spark = spark

    def read_csv(self, path: str, header="false") -> DataFrame:
        """This is a method to read the source in parquet format.

        Args:
            path (str): Path of the source.
            header (str, optional): Set the header true if your document has a header. Defaults to 'false'.

        Returns:
            DataFrame: The source in a data frame format.
        """
        return self.spark.read.option("header", header).csv(path)

    def read_parquet(self, path: str) -> DataFrame:
        """This is a method to read the source in parquet format.

        Args:
            path (str): The path of the source in parquet.

        Returns:
            DataFrame: The source in a data frame format.
        """
        return self.spark.write.parquet(path)

    def write_csv(self, dataframe: DataFrame, path: str, header="false") -> None:
        """This is a method to write a data frame in CSV format.

        Args:
            dataframe (DataFrame): The data frame has to be saved.
            path (str): The path where the data frame has to be saved.
            header (str, optional): Set the header true if you want your document to have a header. Defaults to 'false'.
        """
        dataframe.repartition(1).write.option("header", header).mode("overwrite").csv(
            path
        )

    def write_parquet(self, dataframe: DataFrame, path: str) -> None:
        """This is a method to write data frame in Parquet format.

        Args:
            dataframe (DataFrame): The data frame has to be saved.
            path (str): The path where the data frame has to be saved.
        """
        dataframe.write.parquet(path)
