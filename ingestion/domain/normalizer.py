from dataclasses import dataclass
import datetime
import hashlib
import json
from typing import Optional
import uuid
from enum import Enum

@dataclass
class CodeStep:
    code: str
    instruction: str

@dataclass
class CodeByColumn:
    column_name: str
    code_steps: list[CodeStep]

class ColumnDataType(Enum):
    STRING = "STRING"
    NUMBER = "NUMBER"
    DATE = "DATE"
    DATETIME = "DATETIME"
    BOOLEAN = "BOOLEAN"

@dataclass
class TransformerOutputColumn:
    column_name: str
    data_type: ColumnDataType
    description: str
    is_nullable: bool

    def to_json_dict(self):
        return {
            "columnName": self.column_name,
            "dataType": self.data_type.value,
            "description": self.description,
            "isNullable": self.is_nullable
        }

class TransformerOutputSchema:
    def __init__(self, description: str, columns: list[TransformerOutputColumn]):
        self.description = description
        self.columns = columns or []

    def to_json_dict(self):
        return {
            "description": self.description,
            "columns": [col.to_json_dict() for col in self.columns]
        }

    def hash(self) -> str:
        return hashlib.sha256(json.dumps(self.to_json_dict()).encode("utf-8")).hexdigest()

@dataclass
class NormalizationPipeline:
    normalization_pipeline_id: uuid.UUID
    python_code_by_column: dict[str, list[CodeStep]]
    feedback_or_error: Optional[str]
    previous_version_id: Optional[uuid.UUID]
    created_at: datetime.datetime
    output_schema: TransformerOutputSchema
