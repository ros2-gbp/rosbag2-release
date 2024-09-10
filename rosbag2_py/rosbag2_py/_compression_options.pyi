from typing import ClassVar

FILE: CompressionMode
MESSAGE: CompressionMode
NONE: CompressionMode

class CompressionMode:
    __members__: ClassVar[dict] = ...  # read-only
    FILE: ClassVar[CompressionMode] = ...
    MESSAGE: ClassVar[CompressionMode] = ...
    NONE: ClassVar[CompressionMode] = ...
    __entries: ClassVar[dict] = ...
    def __init__(self, value: int) -> None: ...
    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...
    def __index__(self) -> int: ...
    def __int__(self) -> int: ...
    def __ne__(self, other: object) -> bool: ...
    @property
    def name(self) -> str: ...
    @property
    def value(self) -> int: ...

class CompressionOptions:
    compression_format: str
    compression_mode: CompressionMode
    compression_queue_size: int
    compression_threads: int
    def __init__(self, compression_format: str = ..., compression_mode: CompressionMode = ..., compression_queue_size: int = ..., compression_threads: int = ...) -> None: ...

def compression_mode_from_string(arg0: str) -> CompressionMode: ...
def compression_mode_to_string(arg0: CompressionMode) -> str: ...
