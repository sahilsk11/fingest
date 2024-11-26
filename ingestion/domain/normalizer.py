from dataclasses import dataclass

@dataclass
class CodeStep:
    code: str
    instruction: str

@dataclass
class CodeByColumn:
    column_name: str
    code_steps: list[CodeStep]
