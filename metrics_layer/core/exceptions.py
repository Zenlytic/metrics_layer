class AccessDeniedOrDoesNotExistException(Exception):
    def __init__(self, message: str, object_name: str, object_type: str):
        self.message = message
        self.object_name = object_name
        self.object_type = object_type

    def __str__(self):
        return self.message


class QueryError(Exception):
    def __init__(self, message: str):
        self.message = message

    def __str__(self):
        return self.message


class JoinError(QueryError):
    pass
