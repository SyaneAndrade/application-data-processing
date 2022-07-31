from controller.main_controller import MainController


def main():
    controller = MainController()
    controller.set_polling_orders_processor()
    controller.processing_polling_orders()


if __name__ == "__main__":
    main()
