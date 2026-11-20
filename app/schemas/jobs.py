from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class JobCreate(BaseModel):
    command_type: str = Field(
        min_length=1,
        max_length=100,
    )
    idempotency_key: str = Field(
        min_length=8,
        max_length=128,
    )
    payload: dict[str, Any]


class JobAccepted(BaseModel):
    job_id: UUID
    status: str


class JobRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    job_id: UUID = Field(validation_alias="id")
    command_type: str
    status: str
    attempts: int
    correlation_id: UUID

    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None

    result: dict[str, Any] | None
    error_code: str | None


class JobEventRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    event_id: int = Field(validation_alias="id")
    event_type: str
    details: dict[str, Any]
    created_at: datetime
