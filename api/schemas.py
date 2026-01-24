from pydantic import BaseModel

class HealthResponse(BaseModel):
    ok: bool

from typing import List
from pydantic import BaseModel

class FileMeta(BaseModel):
    filename: str
    content_type: str | None = None

class PhotoEventIn(BaseModel):
    chat_id: str
    files: List[FileMeta]

