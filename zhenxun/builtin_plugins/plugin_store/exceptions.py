class PluginStoreException(Exception):
    def __init__(
        self, message: str, *, code: str | None = None, details: dict | None = None
    ):
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details

    def __str__(self):
        return self.message
