import re
import typing

class Parser():
    def __init__(self):
        super(Parser, self).__init__()
        self.cases = []
        
    def add_case(self, pat:str, consume:typing.Callable[[re.Match], None]) -> None:
        pattern = re.compile(pat)
        self.cases.append((pattern, consume))
        return

    def parse(self, file) -> None:
        for line in file:
            if line.endswith('\n'):
                linet = line[:-1]
            else:
                linet = line
            for case in self.cases:
                match = case[0].fullmatch(linet)
                if match:
                    case[1](match)
        return
    pass
