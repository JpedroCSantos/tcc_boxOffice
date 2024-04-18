# from schema.schema import ApiSchema
from api.schema.schema import ApiSchema
from typing import Optional

class TmdbSchema(ApiSchema):
    budget: float
    runtime: int
    revenue: float
    release_date: int
    vote_average: float
    vote_count: int
    id: int
    original_language: str
    Genre_1: str
    Genre_2: Optional[str]
    Genre_3: Optional[str]
    popularity: float
    Production_Companies: Optional[str]
    Title: str