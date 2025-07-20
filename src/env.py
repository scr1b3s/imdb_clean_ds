import logging
import os
from typing import ClassVar, Union

from environs import Env

env = Env()
env.read_env()  # Reads .env file

from pydantic import BaseModel


class AppEnv(BaseModel):
    DATA_DIR: str = env.str("DATA_DIR")
    IN_DIR: str = env.str("IN_DIR")
    OUT_DIR: str = env.str("OUT_DIR")
    MID_DIR: str = env.str("MID_DIR")
    LIST_PAGE: str = env.str("LIST_PAGE")
    SAVE_DIR: str = env.str("SAVE_DIR")
    CHUNK_SIZE: int = env.int("CHUNK_SIZE")

    _instance: ClassVar[Union["AppEnv", None]] = None

    @classmethod
    def get_instance(cls) -> "AppEnv":
        if cls._instance is None:
            cls._instance = cls._create_new_instance()
        return cls._instance

    @staticmethod
    def _create_new_instance() -> "AppEnv":
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
            raise Exception(
                f"must define the following envs: {', '.join(non_defined_envs)}."
            )
        return AppEnv(**defined_envs)


appEnv = AppEnv.get_instance()
