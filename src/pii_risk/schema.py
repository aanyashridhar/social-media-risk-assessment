"""Schemas for risk assessment records."""

from pydantic import BaseModel


class Record(BaseModel):
    platform: str
    record_type: str
    record_id: str
    author_id_hash: str
    created_at: str
    text: str
