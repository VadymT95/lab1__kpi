from marshmallow import Schema, fields, post_load
from domain.accelerometer import Accelerometer

class AccelerometerSchema(Schema):
    x = fields.Int()
    y = fields.Int()
    z = fields.Int()