from marshmallow import Schema, fields, post_load
from domain.gps import Gps

class GpsSchema(Schema):
    longitude = fields.Number()
    latitude = fields.Number()