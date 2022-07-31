from pyspark.sql import SparkSession


class SparkBuilder(object):
    """This is a class for builder a SparkSession"""

    __spark = SparkSession.builder.appName("App Event Processing").getOrCreate()

    def get_spark(self) -> SparkSession:
        """This is a method to get the value from __spark var.

        Returns:
            SparkSession: The session from spark framework.
        """
        return self.__spark
