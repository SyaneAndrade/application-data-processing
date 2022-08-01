from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, coalesce, lit, date_format
from controller.data_processor.data_event_processor import DataEventProcessor


class PollingOrdersProcessor(DataEventProcessor):
    """Processor for polling events and orders."""

    df_oders: DataFrame = None
    df_polling: DataFrame = None
    df_polling_orders: DataFrame = None

    def set_df_orders(self, df_orders: DataFrame) -> None:
        """Setting the value for dataframe orders.

        Args:
            df_orders (DataFrame): Data frame from orders.
        """
        self.df_oders = df_orders

    def set_df_polling(self, df_polling: DataFrame) -> None:
        """Setting the value for dataframe polling.

        Args:
            df_polling (DataFrame): Data frame from polling events.
        """
        self.df_polling = df_polling

    def join_polling_orders(self) -> DataFrame:
        """Joining the data frame polling events with orders.

        Returns:
            DataFrame: Dataframe from join between polling and orders.
        """
        join_on = self.df_polling.device_id == self.df_oders.device_id
        df_join_polling_orders = self.df_join(
            first_df=self.df_polling,
            second_df=self.df_oders,
            first_alias="polling",
            second_alias="orders",
            join_on=join_on,
            type_join="inner",
        )

        self.df_polling_orders = self.df_polling_orders_select(
            df_polling_orders=df_join_polling_orders
        )
        self.df_polling_orders = self.df_polling_orders.persist()
        self.df_polling_orders.count()
        return self.df_polling_orders

    def create_rank_polling_order(self) -> DataFrame:
        """Creating a column rank for data frame polling order.

        Returns:
            DataFrame: A new data frame with the rank.
        """
        partiton_list = ["order_id"]
        list_order_by = ["creation_time"]
        return self.rank_number(
            df=self.df_polling_orders,
            partition_columns=partiton_list,
            order_columns=list_order_by,
        )

    def filter_time_before_after_order_creation(self) -> DataFrame:
        """Filtering by rank the  time of the polling event immediately preceding,
        and immediately following the order creation time.

        Returns:
            DataFrame: The filtered data frame results.
        """
        select_before_order = self.df_polling_orders.where(
            (
                to_timestamp(col("order_creation_time"))
                < to_timestamp(col("creation_time"))
            )
        )
        rank_before_order = self.rank_number(
            df=select_before_order,
            partition_columns=["order_id"],
            order_columns=["creation_time"],
            order="desc",
            name_column="rank_desc",
        )
        df_before_order = rank_before_order.select(
            col("order_id"),
            col("creation_time").alias("immediately_time_before_order_creation"),
        ).where(col("rank_desc") == 1)
        selec_after_order = self.df_polling_orders.where(
            (
                to_timestamp(col("order_creation_time"))
                > to_timestamp(col("creation_time"))
            )
        )
        rank_after_order = self.rank_number(
            df=selec_after_order,
            partition_columns=["order_id"],
            order_columns=["creation_time"],
        )
        df_after_order = rank_after_order.select(
            col("order_id"),
            col("creation_time").alias("immediately_time_after_order_creation"),
        ).where(col("rank") == 1)

        on_join = "order_id"
        df_join_rank_order_polling = self.df_join(
            first_df=df_before_order,
            second_df=df_after_order,
            first_alias="rank_before_order",
            second_alias="rank_after_order",
            join_on=on_join,
            type_join="full",
        )
        df_join_rank_order_polling = df_join_rank_order_polling.distinct()
        return df_join_rank_order_polling

    def df_polling_orders_select(self, df_polling_orders: DataFrame) -> DataFrame:
        """Selecting columns in data frame object from polling and orders.

        Args:
            df_polling_orders (DataFrame): Data frame object from polling and orders.

        Returns:
            DataFrame: Data frame with select columns.
        """
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
        """Creating a column for the difference value between order creation time and polling creation time for analysis of the frame time.

        Args:
            df_polling_orders (DataFrame): Data frame from orders and polling events.

        Returns:
            DataFrame: A new data frame with a column creation_dates_diff_minutes.
        """
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
        """Filling a value NO ERROR on the column error_code where the value is None.
            When the column error_code is None so it is meant no error occurred.

        Args:
            df (DataFrame): Data frame with polling events and order.

        Returns:
            DataFrame: A new data frame with a column error_code filled.
        """
        return df.withColumn("error_code", coalesce(col("error_code"), lit("NO_ERROR")))

    def count_all_polings_events(self, df: DataFrame, name_column: str) -> DataFrame:
        """Counting all polling events per period.


        Args:
            df (DataFrame): Data frame from polling events per period (-3, +3, -60)minutes from order creation time.
            name_column (str): String for concat in name column the type of time period.

        Returns:
            DataFrame: A new data frame with a column of counts the type of period for polling events.
        """
        cols_list_all_pollings = ["order_id"]
        column_name_polling_events_orders = "count_polling_events_" + name_column
        return self.group_event_count(
            df=df,
            cols_names=cols_list_all_pollings,
            count_column_name=column_name_polling_events_orders,
        )

    def count_status_code(self, df: DataFrame, name_column: str) -> DataFrame:
        """Counting each status code events per period.

        Args:
            df (DataFrame): Data frame from polling events per period (-3, +3, -60)minutes from order creation time.
            name_column (str): String for concat in name column the type of time period.

        Returns:
            DataFrame: A new data frame with a column of counts the type of period for status codes.
        """
        cols_list_each_polling_status_code_orders = ["order_id", "status_code"]
        column_name_each_polling_status_code_orders = "count_status_code_" + name_column
        return self.group_event_count(
            df=df,
            cols_names=cols_list_each_polling_status_code_orders,
            count_column_name=column_name_each_polling_status_code_orders,
        )

    def count_error_code(self, df: DataFrame, name_column: str) -> DataFrame:
        """Counting each error code events per period.

        Args:
            df (DataFrame): Data frame from polling events per period (-3, +3, -60)minutes from order creation time.
            name_column (str): String for concat in name column the type of time period.

        Returns:
            DataFrame: A new data frame with a column of counts the type of period for error codes.
        """
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
        """Selecting columns in data frame object from all events status code.

        Args:
            df (DataFrame): Data frame object from all events status code.
            name_column (str): String used to concat some columns name.

        Returns:
            DataFrame: Data frame with select columns.
        """
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
        """Selecting columns in data frame object from all events status error code.

        Args:
            df (DataFrame): Data frame object from all events status error code.
            name_column (str): String used to concat some columns name.

        Returns:
            DataFrame: Data frame with select columns.
        """
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
        """Return the joining from all counts per period.

        Args:
            df (DataFrame): Data frame from a period (-3, 3, -60 minutes from order time creation).
            name_column (str): String used to concat some columns name.

        Returns:
            DataFrame: Data frame with all counts for the period given.
        """
        # Count the pollings by rule
        df_count_all_polling_events = self.count_all_polings_events(
            df=df, name_column=name_column
        )
        df_status_code = self.count_status_code(df=df, name_column=name_column)
        df_error_code = self.count_error_code(df=df, name_column=name_column)
        # Join all the results
        join_on_all_events_status_code = (
            df_count_all_polling_events.order_id == df_status_code.order_id
        )
        df_join_all_events_status_code = self.df_join(
            first_df=df_count_all_polling_events,
            second_df=df_status_code,
            first_alias="all_events",
            second_alias="all_status_code",
            join_on=join_on_all_events_status_code,
            type_join="full",
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
            type_join="full",
        )
        return self.select_join_all_events_status_error_code(
            df=df_join_all_events_error_code, name_column=name_column
        )

    def select_join_three_minutes_before_after(self, df: DataFrame) -> DataFrame:
        """Selecting columns in data frame object from three minutes before order and three minutes after order.

        Args:
            df (DataFrame): Data frame object from three minutes before order and three minutes after order.

        Returns:
            DataFrame: DataFrame: Data frame with select columns.
        """
        colum_names = [
            "three_minutes_before.order_id",
            "three_minutes_before.status_code",
            "three_minutes_before.error_code",
            "three_minutes_before.count_polling_events_three_minutes_before_order",
            "three_minutes_before.count_status_code_three_minutes_before_order",
            "three_minutes_before.count_error_code_three_minutes_before_order",
            "three_minutes_after.count_polling_events_three_minutes_after_order",
            "three_minutes_after.count_status_code_three_minutes_after_order",
            "three_minutes_after.count_error_code_three_minutes_after_order",
        ]
        alias_list = [
            "order_id",
            "status_code",
            "error_code",
            "count_polling_events_three_minutes_before_order",
            "count_status_code_three_minutes_before_order",
            "count_error_code_three_minutes_before_order",
            "count_polling_events_three_minutes_after_order",
            "count_status_code_three_minutes_after_order",
            "count_error_code_three_minutes_after_order",
        ]
        return self.select_list(df=df, cols_names=colum_names, alias_list=alias_list)

    def select_join_three_sixty_minutes_before_after(self, df: DataFrame) -> DataFrame:
        """Selecting columns in data frame object from three minutes before order, three minutes after order and sixty minutes before order.

        Args:
            df (DataFrame): Data frame object from three minutes before order, three minutes after and sixty minutes before.

        Returns:
            DataFrame: DataFrame: Data frame with select columns.
        """
        column_names = [
            "three_minutes_before_after.order_id",
            "three_minutes_before_after.status_code",
            "three_minutes_before_after.error_code",
            "three_minutes_before_after.count_polling_events_three_minutes_before_order",
            "three_minutes_before_after.count_status_code_three_minutes_before_order",
            "three_minutes_before_after.count_error_code_three_minutes_before_order",
            "three_minutes_before_after.count_polling_events_three_minutes_after_order",
            "three_minutes_before_after.count_status_code_three_minutes_after_order",
            "three_minutes_before_after.count_error_code_three_minutes_after_order",
            "sixty_minutes_before_order.count_polling_events_sixty_minutes_before_order",
            "sixty_minutes_before_order.count_status_code_sixty_minutes_before_order",
            "sixty_minutes_before_order.count_error_code_sixty_minutes_before_order",
        ]
        alias_list = [
            "order_id",
            "status_code",
            "error_code",
            "count_polling_events_three_minutes_before_order",
            "count_status_code_three_minutes_before_order",
            "count_error_code_three_minutes_before_order",
            "count_polling_events_three_minutes_after_order",
            "count_status_code_three_minutes_after_order",
            "count_error_code_three_minutes_after_order",
            "count_polling_events_sixty_minutes_before_order",
            "count_status_code_sixty_minutes_before_order",
            "count_error_code_sixty_minutes_before_order",
        ]
        return self.select_list(df=df, cols_names=column_names, alias_list=alias_list)

    def join_all_counts_periods(
        self,
        df_count_three_minutes_before_order: DataFrame,
        df_count_three_minutes_after_order: DataFrame,
        df_count_sixty_minutes_before_order: DataFrame,
    ) -> DataFrame:
        """Joining for all data frames counted periods.

        Args:
            df_count_three_minutes_before_order (DataFrame): Data frame from a period (-3 from order time creation).
            df_count_three_minutes_after_order (DataFrame): Data frame from a period (3 from order time creation).
            df_count_sixty_minutes_before_order (DataFrame):Data frame from a period (-60 from order time creation).

        Returns:
            DataFrame: Data frame with all counts for all periods given.
        """
        join_on = (
            (
                df_count_three_minutes_before_order.order_id
                == df_count_three_minutes_after_order.order_id
            )
            & (
                df_count_three_minutes_before_order.status_code
                == df_count_three_minutes_after_order.status_code
            )
            & (
                df_count_three_minutes_before_order.error_code
                == df_count_three_minutes_after_order.error_code
            )
        )
        df_join_three_minutes_before_after = self.df_join(
            first_df=df_count_three_minutes_before_order,
            second_df=df_count_three_minutes_after_order,
            first_alias="three_minutes_before",
            second_alias="three_minutes_after",
            join_on=join_on,
            type_join="full",
        )
        df_select_three_minutes_before_after = (
            self.select_join_three_minutes_before_after(
                df=df_join_three_minutes_before_after
            )
        )
        join_on_sixty = (
            (
                df_select_three_minutes_before_after.order_id
                == df_count_sixty_minutes_before_order.order_id
            )
            & (
                df_select_three_minutes_before_after.status_code
                == df_count_sixty_minutes_before_order.status_code
            )
            & (
                df_select_three_minutes_before_after.error_code
                == df_count_sixty_minutes_before_order.error_code
            )
        )
        df_join_three_sixty_minutes_before_after = self.df_join(
            first_df=df_select_three_minutes_before_after,
            second_df=df_count_sixty_minutes_before_order,
            first_alias="three_minutes_before_after",
            second_alias="sixty_minutes_before_order",
            join_on=join_on_sixty,
            type_join="full",
        )
        """ 
        Join the count with all orders with device id in the search, 
        maybe some orders don't have information in the given period.
        """
        df_select_orders = self.df_polling_orders.select(
            "order_id", "order_creation_time"
        ).distinct()
        df_select_three_sixty_minutes_before_after = (
            self.select_join_three_sixty_minutes_before_after(
                df=df_join_three_sixty_minutes_before_after
            )
        )
        df_select_orders_counts = self.df_join(
            first_df=df_select_orders,
            second_df=df_select_three_sixty_minutes_before_after,
            first_alias="orders",
            second_alias="counts",
            join_on="order_id",
            type_join="left",
        )
        return df_select_orders_counts
