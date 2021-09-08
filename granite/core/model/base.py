class GraniteBase:
    def __init__(self, definition: dict = {}) -> None:
        self._definition = definition

    def __getattr__(self, attr: str):
        return self._definition.get(attr, None)

    def __repr__(self):
        return f"<{self.__class__.__name__} name={self.name}>"

    def to_dict(self):
        return self._definition
