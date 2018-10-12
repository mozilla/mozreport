from pathlib import Path
from typing import List, Optional

import attr

from .util import get_data_dir


@attr.s
class Template:
    name: str = attr.ib()
    path: Path = attr.ib()

    @classmethod
    def find_all(cls, user_path: Optional[Path] = None) -> List["Template"]:
        search_path = [
            Path(__file__).parent,
            user_path or get_data_dir(),
        ]
        search_path = [i/"templates" for i in search_path if (i/"templates").exists()]
        discovered = []
        for path in search_path:
            children = (i for i in path.iterdir() if i.is_dir())
            for child in children:
                discovered.append(cls(
                    name=child.name,
                    path=child,
                ))
        return discovered
