class StonesException(Exception):
    def __init__(self, message="Something went wrong") -> None:
        self.message = message
        super().__init__(self.message)
