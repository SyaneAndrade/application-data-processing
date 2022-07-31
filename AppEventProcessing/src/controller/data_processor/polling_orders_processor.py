from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, coalesce, lit
from controller.data_processor.data_event_processor import DataEventProcessor


class PollingOrdersProcessor(DataEventProcessor):
    """Processor for polling events and orders."""

    df_oders: DataFrame = None
    df_polling: DataFrame = None

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
            self.df_polling, self.df_oders, "polling", "orders", join_on
        )

        return self.df_polling_orders_select(df_polling_orders=df_join_polling_orders)

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
        )
        df_select_three_sixty_minutes_before_after = (
            self.select_join_three_sixty_minutes_before_after(
                df=df_join_three_sixty_minutes_before_after
            )
        )
        return df_select_three_sixty_minutes_before_after
