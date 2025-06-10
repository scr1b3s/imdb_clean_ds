import logging
import os
from typing import ClassVar, Union

from dotenv import load_dotenv
from pydantic import BaseModel


class AppEnv(BaseModel):
    DATA_DIR: str
    IN_DIR: str
    OUT_DIR: str
    PROCESS_DIR: str
    LIST_PAGE: str

    _instance: ClassVar[Union["AppEnv", None]] = None

    @classmethod
    def get_instance(cls) -> "AppEnv":
        if cls._instance is None:
            cls._instance = cls._create_new_instance()
        return cls._instance

    @staticmethod
    def _create_new_instance() -> "AppEnv":
        load_dotenv()
        defined_envs = dict()
        non_defined_envs = []
        app_envs = [key for key in AppEnv.model_fields.keys()]
        for key in app_envs:
            env = os.getenv(key)
            if env is None:
                non_defined_envs.append(key)
                continue
            else:
                defined_envs[key] = env
        if len(non_defined_envs) != 0:
            logging.error(
                f"must define the following envs: {' '.join(non_defined_envs)}"
            )
            raise Exception(
                f"must define the following envs: {' '.join(non_defined_envs)}"
            )
        return AppEnv(**defined_envs)


appEnv = AppEnv.get_instance()
