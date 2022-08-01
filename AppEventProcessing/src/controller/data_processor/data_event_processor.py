from select import select
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, dense_rank
from pyspark.sql.window import Window


class DataEventProcessor(object):
    """Class responsible for all the common methods between data processors."""

    def df_period_time(
        self, df: DataFrame, minutes: int, column_period: str
    ) -> DataFrame:
        """Filtering a period of time from a column.

        Args:
            df (DataFrame): Data frame with the column for filtering.
            minutes (int): The value of the period.
            column_period (str): The column is used in the filter clause.

        Returns:
            DataFrame: Dataframe with the filter applied.
        """
        start = 0
        end = minutes
        if minutes < 0:
            start = minutes
            end = 0
        return df.filter(col(column_period).between(start, end))

    def df_join(
        self,
        first_df: DataFrame,
        second_df: DataFrame,
        first_alias: str,
        second_alias: str,
        join_on,
        type_join="inner",
    ) -> DataFrame:
        """Joining two Data frames.

        Args:
            first_df (DataFrame): First data frame for joining.
            second_df (DataFrame): Second data frame for joining.
            first_alias (str): String for an alias for the first data frame.
            second_alias (str): String for an alias for the second data frame.
            join_on (_type_): The conditional for joining the two data frames.
            type_join (str, optional): The type of Join ("left", "right", "full", etc). Defaults to "inner".

        Returns:
            DataFrame: The data frame resulted from joining.
        """
        return first_df.alias(first_alias).join(
            second_df.alias(second_alias), on=join_on, how=type_join
        )

    def group_event_count(
        self, df: DataFrame, cols_names: list, count_column_name
    ) -> DataFrame:
        """Group a data frame based on a list of columns using count aggregate.

        Args:
            df (DataFrame): Dataframe for grouping.
            cols_names (list): List of columns names for grouping.
            count_column_name (_type_): String for renaming the column count results.

        Returns:
            DataFrame: Dataframe is grouped by the list columns.
        """
        group_by_columns = [col(name) for name in cols_names]
        return (
            self.select_list(df=df, cols_names=cols_names)
            .groupBy(*group_by_columns)
            .agg(count("*").alias(count_column_name))
        )

    def select_list(
        self, df: DataFrame, cols_names: list, alias_list=None
    ) -> DataFrame:
        """Selecting columns in data frame object.

        Args:
            df (DataFrame): Data frame object.
            cols_names (list): List with all columns to be selected.
            alias_list (_type_, optional): List with all the new names for columns selected. Defaults to None.

        Returns:
            DataFrame: Data frame with select columns.
        """
        select_column_names = []
        if alias_list:
            select_column_names = [
                col(cols_names[i]).alias(alias_list[i])
                for i in range(0, len(cols_names))
            ]
        else:
            select_column_names = [col(name) for name in cols_names]
        return df.select(*select_column_names)

    def rank_number(
        self,
        df: DataFrame,
        partition_columns: list,
        order_columns: list,
        order: str = "asc",
        name_column: str = "rank",
    ) -> DataFrame:
        partition_columns = [col(name) for name in partition_columns]
        if order == "desc":
            order_columns = [col(name).desc() for name in order_columns]
        else:
            order_columns = [col(name) for name in order_columns]
        window = Window.partitionBy(*partition_columns).orderBy(*order_columns)
        return df.withColumn(name_column, dense_rank().over(window))
