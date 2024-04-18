from api.schema.schema import ApiSchema
# from schema.schema import ApiSchema
from typing import Optional

class OmdbSchema(ApiSchema):
    Director: Optional[str]
    Actors: Optional[str]
    Metascore: Optional[float]
    imdbRating: Optional[float]
    BoxOffice: Optional[float]
