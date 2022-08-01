from controller.main_controller import MainController


def main():
    controller = MainController()
    controller.set_polling_orders_processor()
    controller.set_connectivity_order()
    controller.processing_polling_orders()
    controller.time_immediately_before_after_order()
    controller.processing_connectivity_order()
    controller.join_all_df_processed()
    controller.save()
    controller.unpersist_data_frames()


if __name__ == "__main__":
    main()
