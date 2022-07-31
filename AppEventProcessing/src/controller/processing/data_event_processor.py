from select import select
from pyspark.sql import DataFrame, GroupedData
from pyspark.sql.functions import col, count
from pyspark.sql.types import *


class DataEventProcessor(object):
    def df_period_time(
        self, df: DataFrame, minutes: int, column_period: str
    ) -> DataFrame:
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
        return first_df.alias(first_alias).join(
            second_df.alias(second_alias), on=join_on, how=type_join
        )

    def group_event_count(
        self, df: DataFrame, cols_names: list, count_column_name
    ) -> DataFrame:
        group_by_columns = [col(i) for i in cols_names]
        return (
            self.select_list(df=df, cols_names=cols_names)
            .groupBy(*group_by_columns)
            .agg(count("*").alias(count_column_name))
        )

    def select_list(
        self, df: DataFrame, cols_names: list, alias_list=None
    ) -> DataFrame:
        select_column_names = []
        if alias_list:
            select_column_names = [
                col(cols_names[i]).alias(alias_list[i])
                for i in range(0, len(cols_names))
            ]
        else:
            select_column_names = [col(name) for name in cols_names]
        return df.select(*select_column_names)
