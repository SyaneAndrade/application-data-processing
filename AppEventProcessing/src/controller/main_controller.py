from dao.data_acess_object import DataAcessObject
from config.config import Config
from dependencies.spark_builder import SparkBuilder
from pyspark.sql import DataFrame
from controller.data_processor.polling_orders_processor import PollingOrdersProcessor
from controller.data_processor.connectivity_order_processor import (
    ConnectivityOrderProcessor,
)


class MainController(object):
    """The class that controllers the entire project"""

    def __init__(self) -> None:
        self.spark: SparkBuilder = SparkBuilder().get_spark()
        self.dao: DataAcessObject = DataAcessObject(self.spark)
        self.conf: Config = Config().get_config()
        self.polling_order_processor: PollingOrdersProcessor = PollingOrdersProcessor()
        self.connectivity_order_processor: ConnectivityOrderProcessor = (
            ConnectivityOrderProcessor()
        )
        self.df_processed_count_polling_orders: DataFrame = None
        self.df_time_immediately_before_after_order: DataFrame = None
        self.df_status_before_order: DataFrame = None
        self.df_to_save: DataFrame = None

    def read_connectivity_status(self) -> DataFrame:
        """The method that read the source connectivity_status.

        Returns:
            DataFrame: The data frame to connectivity_status.
        """
        return self.dao.read_csv(self.conf["path_connectivity_status"], "true")

    def read_orders(self) -> DataFrame:
        """The method that read the source orders.

        Returns:
            DataFrame: The data frame to orders.
        """
        return self.dao.read_csv(self.conf["path_orders"], "true")

    def read_polling(self) -> DataFrame:
        """The method that read the source polling.

        Returns:
            DataFrame: The data frame to polling.
        """
        return self.dao.read_csv(self.conf["path_polling"], "true")

    def set_polling_orders_processor(self) -> None:
        """Setting the data frames for an object of processing pollings and orders."""
        self.polling_order_processor.set_df_orders(df_orders=self.read_orders())
        self.polling_order_processor.set_df_polling(df_polling=self.read_polling())

    def processing_polling_orders(self):
        """Controlling the main logic to obtain:
          ??? The total count of all polling events;
          ??? The count of each type of polling status_code;
          ??? The count of each type of polling error_code and the count of responses without error.
          codes.
        Per period:
          ??? Three minutes before the order creation time;
          ??? Three minutes after the order creation time;
          ??? One hour before the order creation time.
        """
        df_polling_orders = self.polling_order_processor.join_polling_orders()
        df_diff_date_creation_polling_orders = (
            self.polling_order_processor.diff_date_creation_polling_orders(
                df_polling_orders=df_polling_orders
            )
        )
        df_diff_date_creation_polling_orders = (
            df_diff_date_creation_polling_orders.cache()
        )
        df_diff_date_creation_polling_orders.count()
        df_three_minutes_before_order = self.polling_order_processor.df_period_time(
            df=df_diff_date_creation_polling_orders,
            minutes=-3,
            column_period="creation_dates_diff_minutes",
        )
        df_sixty_minutes_before_order = self.polling_order_processor.df_period_time(
            df=df_diff_date_creation_polling_orders,
            minutes=-60,
            column_period="creation_dates_diff_minutes",
        )
        df_three_minutes_after_order = self.polling_order_processor.df_period_time(
            df=df_diff_date_creation_polling_orders,
            minutes=3,
            column_period="creation_dates_diff_minutes",
        )
        df_count_three_minutes_before_order = (
            self.polling_order_processor.df_count_all_events_polling_period(
                df=df_three_minutes_before_order,
                name_column="three_minutes_before_order",
            )
        )
        df_count_sixty_minutes_before_order = (
            self.polling_order_processor.df_count_all_events_polling_period(
                df=df_sixty_minutes_before_order,
                name_column="sixty_minutes_before_order",
            )
        )
        df_count_three_minutes_after_order = (
            self.polling_order_processor.df_count_all_events_polling_period(
                df=df_three_minutes_after_order,
                name_column="three_minutes_after_order",
            )
        )
        df_join_three_sity_minutes_before_after = (
            self.polling_order_processor.join_all_counts_periods(
                df_count_three_minutes_before_order=df_count_three_minutes_before_order,
                df_count_three_minutes_after_order=df_count_three_minutes_after_order,
                df_count_sixty_minutes_before_order=df_count_sixty_minutes_before_order,
            )
        )

        self.df_processed_count_polling_orders = (
            df_join_three_sity_minutes_before_after.persist()
        )
        self.df_processed_count_polling_orders.count()
        df_diff_date_creation_polling_orders.unpersist()

    def time_immediately_before_after_order(self):
        """Controlling the main logic to obtain The time of the polling event immediately preceding,
        and immediately following the order creation time.
        """
        self.df_time_immediately_before_after_order = (
            self.polling_order_processor.filter_time_before_after_order_creation()
        )
        self.df_time_immediately_before_after_order.persist()
        self.df_time_immediately_before_after_order.count()

    def set_connectivity_order(self) -> None:
        self.connectivity_order_processor.set_orders(df_orders=self.read_orders())
        self.connectivity_order_processor.set_connectivity_status(
            df_connectivity_status=self.read_connectivity_status()
        )
        self.connectivity_order_processor.set_connectivity_orders()

    def processing_connectivity_order(self) -> None:
        """Controlling the main logic to obtain most recent connectivity status."""
        self.connectivity_order_processor.rank_status_connectivity_orders()
        self.connectivity_order_processor.df_connectivity_orders.cache()
        self.connectivity_order_processor.df_connectivity_orders.count()
        self.df_status_before_order = (
            self.connectivity_order_processor.filter_status_before_order()
        )
        self.df_status_before_order.persist()
        self.df_status_before_order.count()
        self.connectivity_order_processor.df_connectivity_orders.unpersist()

    def join_all_df_processed(self) -> None:
        """Create the final data frame with all information processed."""
        join_on = "order_id"
        df_final = self.polling_order_processor.df_join(
            first_df=self.df_processed_count_polling_orders,
            second_df=self.df_time_immediately_before_after_order,
            first_alias="counts",
            second_alias="time_before",
            join_on=join_on,
            type_join="left",
        )
        self.df_to_save = self.polling_order_processor.df_join(
            first_df=df_final,
            second_df=self.df_status_before_order,
            first_alias="final",
            second_alias="status_before",
            join_on=join_on,
            type_join="left",
        )

    def save(self) -> None:
        """Save the data frame final."""
        self.dao.write_csv(
            dataframe=self.df_to_save, path=self.conf["save_file"], header="true"
        )

    def unpersist_data_frames(self) -> None:
        """Unpersist the data frames in memory"""
        self.df_time_immediately_before_after_order.unpersist()
        self.df_processed_count_polling_orders.unpersist()
        self.polling_order_processor.df_polling_orders.unpersist()
        self.df_status_before_order.unpersist()
