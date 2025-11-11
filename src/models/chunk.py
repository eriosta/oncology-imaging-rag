from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime


class Chunk(BaseModel):
    """Standardized chunk model for all document types"""
    
    chunk_id: str = Field(..., description="Unique identifier for this chunk")
    text: str = Field(..., description="The actual text content to be embedded")
    source_type: str = Field(..., description="Type: radlex|loinc|tnm_table|tnm_guideline|recist")
    
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Flexible metadata storage")
    
    # Common metadata fields (stored in metadata dict):
    # - source_file: str
    # - created_at: datetime
    # - chunk_method: str (how this was chunked)
    # - cancer_type: Optional[str] (for TNM)
    # - term_id: Optional[str] (for RadLex/LOINC)
    # - category: Optional[str]
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSONL storage"""
        return self.model_dump()
    
    @classmethod
    def from_dict(cls, data: dict):
        """Load from dictionary"""
        return cls(**data)
