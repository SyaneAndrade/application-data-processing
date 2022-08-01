from controller.main_controller import MainController


def main():
    controller = MainController()
    controller.set_polling_orders_processor()
    controller.processing_polling_orders()
    controller.time_immediately_before_after_order()
    controller.unpersist_data_frames()


if __name__ == "__main__":
    main()
