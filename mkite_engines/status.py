from enum import Enum


class Status(Enum):
    BUILDING = "building"
    READY = "ready"
    DOING = "doing"
    DONE = "done"
    ERROR = "error"
    PARSING = "parsing"
    ANY = "any"
    ARCHIVE = "archive"
