import os
from mkite_core.external import load_config
from pydantic import BaseSettings, Field, DirectoryPath, FilePath


class EngineSettings(BaseSettings):
    """Wraps and obtains all settings for the environmental variables"""

    class Config:
        case_sensitive = False

    @classmethod
    def from_file(cls, filename: FilePath):
        data = load_config(filename)
        return cls(**data)
