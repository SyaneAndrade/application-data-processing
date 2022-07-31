from typing import Dict
from yaml import safe_load
import os.path


class Config(object):
    """This is a class to built the config object."""

    __conf: Dict = {}

    def get_config(self) -> Dict:
        """This is a method that creates and returns the dict config.

        Returns:
            Dict: Return the config in a dict format.
        """
        config_yaml = self.__read_yaml()
        self.__create_read_config(config_yaml)
        self.__create_save_config(config_yaml)
        return self.__conf

    def __read_yaml(self) -> Dict:
        """This is a method that read a Yaml file.

        Returns:
            Dict: Return the yaml content in a dict format.
        """
        file_folder = os.path.join("src", "config")
        file_to_open = os.path.join(file_folder, "config.yaml")
        config_file = open(file_to_open, "r")
        return safe_load(config_file)

    def __create_read_config(self, config_yaml: Dict) -> None:
        """This method creates the read paths config.

        Args:
            config_yaml (Dict):  The dict that contains the YAML content.
        """
        config_read_path = config_yaml["path_source"]
        read_file_folder = os.path.join("src", config_read_path["folder"])
        self.__conf["path_connectivity_status"] = os.path.join(
            read_file_folder, config_read_path["connectivity_status"]
        )
        self.__conf["path_orders"] = os.path.join(
            read_file_folder, config_read_path["orders"]
        )
        self.__conf["path_polling"] = os.path.join(
            read_file_folder, config_read_path["polling"]
        )

    def __create_save_config(self, config_yaml: Dict) -> None:
        """This method creates the save paths config.

        Args:
            config_yaml (Dict): The dict that contains the YAML content.
        """
        config_save_path = config_yaml["path_to_save"]
        save_file_folder = os.path.join(
            "src", config_save_path["folder"][0], config_save_path["folder"][1]
        )
        self.__conf["save_file"] = os.path.join(
            save_file_folder, config_save_path["save_file"]
        )
