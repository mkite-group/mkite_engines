import os
from mkite_core.external import load_config
from pydantic import Field, DirectoryPath, FilePath
from pydantic_settings import BaseSettings, SettingsConfigDict


class EngineSettings(BaseSettings):
    """Wraps and obtains all settings for the environmental variables"""
    model_config = SettingsConfigDict(case_sensitive=False)

    @classmethod
    def from_file(cls, filename: FilePath):
        data = load_config(filename)

        if "_module" in data:
            data.pop("_module")

        return cls(**data)
