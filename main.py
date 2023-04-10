from fastapi import FastAPI, Query, HTTPException
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from typing import Optional
import uvicorn
from configg import *
from classes import *

# create database engine and session
engine = create_engine(connect_str)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# create fastapi instance
app = FastAPI()


@app.get('/')
def root():
    return "Welcome to the fastAPI demo implementation!"


@app.get('/getAll')
def getAll():
    '''get the data of all the people'''
    with SessionLocal() as session:
        people = session.query(Person).all()
        for person in people:
            yield person


# get person with path parameter
@app.get('/person/{p_id}', status_code=200)
def get_person(p_id: int):
    with SessionLocal() as session:
        person = session.query(Person).filter(Person.id == p_id).first()
        if not person:
            raise HTTPException(status_code=404, detail="Person not found")
        return {"id": person.id, "name": person.name, "age": person.age, "gender": person.gender}


# get person using Query parameter
@app.get('/search', status_code=200)
def search_person(age: Optional[int] = Query(None, title="Age", description="the age of the person"),
                  name: Optional[str] = Query(None, title="Name", description="the name of the person")):
    with SessionLocal() as session:
        person = session.query(Person).filter(Person.age == age).filter(Person.name == name).first()
        if not person:
            raise HTTPException(status_code=404, detail="Person not found")
        return {"id": person.id, "name": person.name, "age": person.age, "gender": person.gender}


@app.post('/addPerson', response_model=PersonModel, status_code=201)
def add_person(person: PersonModel):
    '''
    "expire_on_commit=False" is a important parameter for session as
    objects get expired for example after committing, then when such expired objects are about
    to get used the ORM tries to refresh them, but this cannot be done when objects are detached
    from session (e.g. because that session was closed). This behavior can be managed by creating
    session with expire_on_commit=False param.
    '''
    with SessionLocal(expire_on_commit=False) as session: 
        new_person = Person(name=person.name, age=person.age, gender=person.gender)
        session.add(new_person)
        session.commit()
        return new_person


@app.put('/changePerson/{id}', response_model=PersonModel, status_code=200)
def change_person(id:int,person: PersonModel):
    with SessionLocal() as session:
        try:
            session.query(Person).filter(Person.id == id).update({"name": person.name, "age":person.age, "gender": person.gender})
            session.commit()
            return person
        except SQLAlchemyError as e:
            session.rollback()
            raise HTTPException(status_code=500, detail=str(e))
    

@app.delete('/delete/{p_id}')
def delete_person(p_id: int):
    with SessionLocal() as session:
        person = session.query(Person).filter(Person.id == p_id).first()
        if not person:
            raise HTTPException(status_code=404, detail="Person not found")
        session.delete(person)
        session.commit()
        return f"Person with id '{p_id}' has been deleted successfully!"


if __name__ == "__main__":
    Base.metadata.create_all(bind=engine,checkfirst=True) # create table if not exist
    '''
    sqlalchemy.schema.MetaData.create_all(bind=None, tables=None, checkfirst=True)
    eg. sqlalchemy.schema.MetaData.create_all(bind=engine, tables=['userTable','demoTable'], checkfirst=True)
    This statement create all tables stored in this metadata.
    Conditional by default, will not attempt to recreate tables already present in the target database.
    Parameters:
    bind - A Connectable used to access the database; if None, uses the existing bind on this MetaData, if any.
    tables - Optional list of Table objects, which is a subset of the total tables in the MetaData (others are ignored).
    checkfirst - Defaults to True, don't issue CREATEs for tables already present in the target database.
    '''
    uvicorn.run(app, host="0.0.0.0", port=8000)
