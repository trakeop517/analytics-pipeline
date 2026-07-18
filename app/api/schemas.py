from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field

class EventCreate(BaseModel):
    source: str = Field(
        ..., 
        max_length=50, 
        json_schema_extra={"example": "github"}, 
        description="Источник события (github, reddit, etc.)"
    )
    external_id: str = Field(
        ..., 
        max_length=255, 
        json_schema_extra={"example": "issue_12345"}, 
        description="ID события во внешней системе"
    )
    title: Optional[str] = Field(
        None, 
        max_length=500, 
        json_schema_extra={"example": "New bug report fixed"}, 
        description="Заголовок события"
    )
    payload: Dict[str, Any] = Field(
        ..., 
        json_schema_extra={"example": {"author": "artur", "stars": 150}}, 
        description="Полный JSON со всеми сырыми данными от источника"
    )
    created_at: datetime = Field(
        default_factory=datetime.utcnow, 
        description="Время создания события на источнике"
    )