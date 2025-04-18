class SequentialCompressionReader:
    def __init__(self) -> None: ...
    def close(self) -> None: ...
    def get_all_message_definitions(self, *args, **kwargs): ...
    def get_all_topics_and_types(self, *args, **kwargs): ...
    def get_metadata(self, *args, **kwargs): ...
    def has_next(self) -> bool: ...
    def open(self, arg0, arg1) -> None: ...
    def open_uri(self, arg0: str) -> None: ...
    def read_next(self) -> tuple: ...
    def reset_filter(self) -> None: ...
    def seek(self, arg0: int) -> None: ...
    def set_filter(self, arg0) -> None: ...
    def set_read_order(self, arg0) -> bool: ...

class SequentialReader:
    def __init__(self) -> None: ...
    def close(self) -> None: ...
    def get_all_message_definitions(self, *args, **kwargs): ...
    def get_all_topics_and_types(self, *args, **kwargs): ...
    def get_metadata(self, *args, **kwargs): ...
    def has_next(self) -> bool: ...
    def open(self, arg0, arg1) -> None: ...
    def open_uri(self, arg0: str) -> None: ...
    def read_next(self) -> tuple: ...
    def reset_filter(self) -> None: ...
    def seek(self, arg0: int) -> None: ...
    def set_filter(self, arg0) -> None: ...
    def set_read_order(self, arg0) -> bool: ...

def get_registered_readers() -> Set[str]: ...
