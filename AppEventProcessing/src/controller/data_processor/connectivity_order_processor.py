from numpy import partition
from controller.data_processor.data_event_processor import DataEventProcessor
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, coalesce, lit, date_format


class ConnectivityOrderProcessor(DataEventProcessor):
    df_oders: DataFrame = None
    df_connectivity_status: DataFrame = None
    df_connectivity_orders: DataFrame = None

    def set_orders(self, df_orders: DataFrame) -> None:
        self.df_oders = df_orders

    def set_connectivity_status(self, df_connectivity_status: DataFrame) -> None:
        self.df_connectivity_status = df_connectivity_status

    def select_connectivity_order(self, df: DataFrame) -> DataFrame:
        column_names = [
            "connectivity_status.creation_time",
            "connectivity_status.status",
            "connectivity_status.device_id",
            "orders.order_creation_time",
            "orders.order_id",
        ]
        alias_list = [
            "creation_time",
            "status",
            "device_id",
            "order_creation_time",
            "order_id",
        ]
        return self.select_list(df=df, cols_names=column_names, alias_list=alias_list)

    def set_connectivity_orders(self) -> None:
        on_join = self.df_connectivity_status.device_id == self.df_oders.device_id
        df_join_connectivity_orders = self.df_join(
            first_df=self.df_connectivity_status,
            second_df=self.df_oders,
            first_alias="connectivity_status",
            second_alias="orders",
            join_on=on_join,
        )
        self.df_connectivity_orders = self.select_connectivity_order(
            df=df_join_connectivity_orders
        )
        self.fill_status_connectivity_orders()
        self.rank_status_connectivity_orders()

    def fill_status_connectivity_orders(self) -> None:
        self.df_connectivity_orders = self.df_connectivity_orders.withColumn(
            "status", coalesce(col("status"), lit("NO_CONNECTIVITY"))
        )

    def rank_status_connectivity_orders(self) -> None:
        partition_list = ["order_id"]
        order_by_list = ["creation_time"]
        self.df_connectivity_orders = self.rank_number(
            df=self.df_connectivity_orders,
            partition_columns=partition_list,
            order_columns=order_by_list,
        )
        self.df_connectivity_orders

    def select_rank_order_desc(self, df: DataFrame) -> DataFrame:
        columns_name = ["order_id", "status", "creation_time", "device_id"]
        alias_list = [
            "order_id",
            "status_before_order",
            "creation_time_status_before_order",
            "device_id",
        ]
        return self.select_list(df=df, cols_names=columns_name, alias_list=alias_list)

    def filter_status_before_order(self) -> DataFrame:
        df_rank_order = self.df_connectivity_orders.where(
            date_format(col("creation_time"), "yyyy-MM-dd HH:MM:SS")
            < col("order_creation_time")
        )
        df_rank_order_desc = self.rank_number(
            df=df_rank_order,
            partition_columns=["order_id"],
            order_columns=["creation_time"],
            order="desc",
            name_column="rank_desc",
        )
        df_filter_rank = df_rank_order_desc.where(col("rank_desc") == 1)
        df_select_desc_rank = self.select_rank_order_desc(df_filter_rank)
        return df_select_desc_rank

    def select_status_before_order(self, df: DataFrame) -> DataFrame:
        columns_name = [
            "connectivity_orders.creation_time",
            "connectivity_orders.status",
            "connectivity_orders.device_id",
            "connectivity_orders.order_creation_time",
            "connectivity_orders.order_id",
            "status_before_order.status_before_order",
            "status_before_order.creation_time_status_before_order",
        ]
        alias_list = [
            "creation_time",
            "status",
            "device_id",
            "order_creation_time",
            "order_id",
            "status_before_order",
            "creation_time_status_before_order",
        ]
        return self.select_list(df=df, cols_names=columns_name, alias_list=alias_list)

    def filter_change_status_before_order(
        self, df_select_desc_rank: DataFrame
    ) -> DataFrame:
        on_join = (
            self.df_connectivity_orders.order_id == df_select_desc_rank.order_id
        ) & (self.df_connectivity_orders.device_id == df_select_desc_rank.device_id)
        df_join_status_before_order = self.df_join(
            first_df=self.df_connectivity_orders,
            second_df=df_select_desc_rank,
            first_alias="connectivity_orders",
            second_alias="status_before_order",
            join_on=on_join,
        )
        df_select_status_before_order = self.select_status_before_order(
            df=df_join_status_before_order
        )
        df_other_status = df_select_status_before_order.filter(
            (col("status_before_order") != col("status"))
            & (col("creation_time") < col("creation_time_status_before_order"))
        )
        df_other_status_rank_desc = self.rank_number(
            df=df_other_status,
            partition_columns=["order_id"],
            order_columns=["creation_time"],
            order="desc",
            name_column="rank_desc",
        )
        df_last_time_other_status = df_other_status_rank_desc.where(
            col("rank_desc") == 1
        )
        df_last_time_other_status.show(truncate=False)
