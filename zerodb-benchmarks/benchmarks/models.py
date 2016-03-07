# Test models
from zerodb.models import Model, Field, Text


class TestRecord(Model):
    text = Text()
    int_val = Field()
    float_val = Field()


class WikiPage(Model):
    id = Field()
    title = Field()
    text = Text()
