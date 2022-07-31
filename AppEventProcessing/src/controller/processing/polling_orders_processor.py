from numpy import column_stack
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format, to_timestamp, coalesce, lit
from controller.processing.data_event_processor import DataEventProcessor


class PollingOrdersProcessor(DataEventProcessor):
    df_oders: DataFrame = None
    df_polling: DataFrame = None

    def set_df_orders(self, df_orders: DataFrame) -> None:
        self.df_oders = df_orders

    def set_df_polling(self, df_polling: DataFrame) -> None:
        self.df_polling = df_polling

    def parse_datetime_polling(self):
        self.df_polling = self.df_polling.withColumnRenamed(
            "creation_time", "creation_time_mil_seconds"
        )
        self.df_polling = self.df_polling.withColumn(
            "creation_time",
            date_format(col("creation_time_mil_seconds"), "yyyy-MM-dd HH:MM:SS"),
        )

    def join_polling_orders(self) -> DataFrame:
        join_on = self.df_polling.device_id == self.df_oders.device_id
        return self.df_join(
            self.df_polling, self.df_oders, "polling", "orders", join_on
        )

    def df_polling_orders_select(self, df_polling_orders: DataFrame) -> DataFrame:
        column_names = [
            "polling.creation_time",
            "polling.device_id",
            "polling.error_code",
            "polling.status_code",
            "orders.order_creation_time",
            "orders.order_id",
        ]
        alias_list = [
            "creation_time",
            "device_id",
            "error_code",
            "status_code",
            "order_creation_time",
            "order_id",
        ]
        return self.select_list(
            df=df_polling_orders, cols_names=column_names, alias_list=alias_list
        )

    def diff_date_creation_polling_orders(
        self, df_polling_orders: DataFrame
    ) -> DataFrame:
        return df_polling_orders.withColumn(
            "creation_dates_diff_minutes",
            (
                (
                    to_timestamp(("order_creation_time")).cast("long")
                    - to_timestamp(col("creation_time")).cast("long")
                )
                / 60
            ).cast("int"),
        )

    def df_with_error_code(self, df: DataFrame) -> DataFrame:
        return df.withColumn("error_code", coalesce(col("error_code"), lit("NO_ERROR")))

    def count_all_polings_events(self, df: DataFrame, name_column: str) -> DataFrame:
        cols_list_all_pollings = ["order_id"]
        column_name_polling_events_orders = "count_polling_events_" + name_column
        return self.group_event_count(
            df=df,
            cols_names=cols_list_all_pollings,
            count_column_name=column_name_polling_events_orders,
        )

    def count_status_code(self, df: DataFrame, name_column: str) -> DataFrame:
        cols_list_each_polling_status_code_orders = ["order_id", "status_code"]
        column_name_each_polling_status_code_orders = "count_status_code_" + name_column
        return self.group_event_count(
            df=df,
            cols_names=cols_list_each_polling_status_code_orders,
            count_column_name=column_name_each_polling_status_code_orders,
        )

    def count_error_code(self, df: DataFrame, name_column: str) -> DataFrame:
        df_with_error = self.df_with_error_code(df=df)
        cols_list_each_polling_error = ["order_id", "status_code", "error_code"]
        column_name_each_polling_error_code_orders = "count_error_code_" + name_column
        return self.group_event_count(
            df=df_with_error,
            cols_names=cols_list_each_polling_error,
            count_column_name=column_name_each_polling_error_code_orders,
        )

    def select_join_all_events_status_code(
        self, df: DataFrame, name_column: str
    ) -> DataFrame:
        column_names = [
            "all_events.order_id",
            "all_status_code.status_code",
            "all_events.count_polling_events_" + name_column,
            "all_status_code.count_status_code_" + name_column,
        ]

        alias_list = [
            "order_id",
            "status_code",
            "count_polling_events_" + name_column,
            "count_status_code_" + name_column,
        ]
        return self.select_list(df=df, cols_names=column_names, alias_list=alias_list)

    def select_join_all_events_status_error_code(
        self, df: DataFrame, name_column: str
    ) -> DataFrame:
        column_names = [
            "all_events_status_code.order_id",
            "all_events_status_code.status_code",
            "error_code.error_code",
            "all_events_status_code.count_polling_events_" + name_column,
            "all_events_status_code.count_status_code_" + name_column,
            "error_code.count_error_code_" + name_column,
        ]

        alias_list = [
            "order_id",
            "status_code",
            "error_code",
            "count_polling_events_" + name_column,
            "count_status_code_" + name_column,
            "count_error_code_" + name_column,
        ]
        return self.select_list(df=df, cols_names=column_names, alias_list=alias_list)

    def df_count_all_events_polling_period(
        self, df: DataFrame, name_column: str
    ) -> DataFrame:
        df_count_all_polling_events = self.count_all_polings_events(
            df=df, name_column=name_column
        )
        df_status_code = self.count_status_code(df=df, name_column=name_column)
        df_error_code = self.count_error_code(df=df, name_column=name_column)
        join_on_all_events_status_code = (
            df_count_all_polling_events.order_id == df_status_code.order_id
        )
        df_join_all_events_status_code = self.df_join(
            first_df=df_count_all_polling_events,
            second_df=df_status_code,
            first_alias="all_events",
            second_alias="all_status_code",
            join_on=join_on_all_events_status_code,
        )
        df_all_events_status_code = self.select_join_all_events_status_code(
            df=df_join_all_events_status_code, name_column=name_column
        )
        join_on_all_events_error_code = (
            df_all_events_status_code.order_id == df_error_code.order_id
        ) & ((df_all_events_status_code.status_code == df_error_code.status_code))
        df_join_all_events_error_code = self.df_join(
            first_df=df_all_events_status_code,
            second_df=df_error_code,
            first_alias="all_events_status_code",
            second_alias="error_code",
            join_on=join_on_all_events_error_code,
        )
        return self.select_join_all_events_status_error_code(df=df_join_all_events_error_code, name_column=name_column)
