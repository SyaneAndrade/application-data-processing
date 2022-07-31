from dao.data_acess_object import DataAcessObject
from config.config import Config
from dependencies.spark_builder import SparkBuilder
from pyspark.sql import DataFrame
from controller.data_processor.polling_orders_processor import PollingOrdersProcessor


class MainController(object):
    """The class that controllers the entire project"""

    def __init__(self) -> None:
        self.spark = SparkBuilder().get_spark()
        self.dao = DataAcessObject(self.spark)
        self.conf = Config().get_config()
        self.polling_order_processor = PollingOrdersProcessor()

    def read_source_connectivity_status(self) -> DataFrame:
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
        self.polling_order_processor.set_df_orders(self.read_orders())
        self.polling_order_processor.set_df_polling(self.read_polling())

    def processing_polling_orders(self):
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

        df_join_three_sity_minutes_before_after.show(truncate=False)
        df_diff_date_creation_polling_orders.unpersist()
