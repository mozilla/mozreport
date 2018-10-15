from os import walk
from pathlib import Path
from typing import List, Optional

import attr

from ._version import __version__
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

    def emplace(self, target: Path, overwrite: bool = True) -> None:
        if not target.exists():
            raise FileNotFoundError(str(target))
        if not target.is_dir():
            raise NotADirectoryError(str(target))

        def transform(x):
            return x.replace(b"MOZREPORT_VERSION", __version__.public().encode("ascii"))

        for (path, dirs, files) in walk(self.path):
            path = Path(path)
            relative = path.relative_to(self.path)
            (target / relative).mkdir(exist_ok=True)
            for filename in files:
                dest = target/relative/filename
                if not overwrite and dest.exists():
                    raise FileExistsError(str(dest))
                buffer = (path/filename).read_bytes()
                buffer = transform(buffer)
                dest.write_bytes(buffer)
