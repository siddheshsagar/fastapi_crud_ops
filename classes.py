from pydantic import BaseModel
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base
import uuid, json

# from sqlalchemy.orm import MetaData, Table
Base = declarative_base()

# generate unique ids
def generate_uuid():
    return (uuid.uuid4().int & (1<<16)-1)

# pydentic model
class PersonModel(BaseModel):
    '''
    Pydantic's orm_mode will tell the Pydantic model to read the data even if it is not a dict,
    but an ORM model (or any other arbitrary object with attributes).
    This way, instead of only trying to get the id value from a dict, as in: id = data["id"],
    it will also try to get it from an attribute, as in: id = data.id
    '''
    name: str
    age: int
    gender: str
    
    class Config:
        orm_mode = True
        
    
# declarative base model
class Person(Base):
    __tablename__ = "fastDemo" 

    id = Column(Integer, primary_key=True, default=generate_uuid())   
    name = Column(String(50))
    age = Column(Integer)
    gender = Column(String(50))