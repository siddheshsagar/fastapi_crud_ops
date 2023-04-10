from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from typing import Optional
import pyodbc
import uvicorn
from configg import *
from classes import PersonModel, generate_uuid


# database connection
config = json.loads(get_connection_config())
conn = pyodbc.connect('DRIVER='+config["driver"]+';SERVER='+config["server"]+';PORT=1433;DATABASE='+config["database"]+';UID='+config["username"]+';PWD='+ config["password"]) 
cursor = conn.cursor()

# create fastapi instance
app = FastAPI()


@app.get('/')
def root():
    return "Welcome to the fastAPI demo implementation!"


# fetch all the people
@app.get('/getAll')
def getAll():
    query = f"SELECT * FROM fastapi_demo"
    cursor.execute(query)
    
    columns = [column[0] for column in cursor.description]
    results = []
    for row in cursor.fetchall():
        results.append(dict(zip(columns, row)))
    return results
   
 
        
#get person with p_id as a path parameter
@app.get('/person/{p_id}',status_code=200)
def get_person(p_id:int):
    query = f"SELECT * FROM fastapi_demo WHERE id = {p_id}"
    cursor.execute(query)
    data = cursor.fetchall()
    print(data)
    if data:
        data = data[0]
    else:
        raise HTTPException(status_code=404, detail="Person not found")
    
    return {"id":data[0],"name":data[1],"age":data[2],"gender":data[3]} if data else {}



# get person using Query parameter
@app.get('/search', status_code=200)
def search_person(age: int = Query(default=None, title="Age", description="the age of the person"),
                  name: str = Query(default=None, title="Name", description="the name of the person")):
    query = f"SELECT * FROM fastapi_demo WHERE age = {age}"
    cursor.execute(query)
    data = cursor.fetchall()
    if data:
        data = data[0]
    else:
        raise HTTPException(status_code=404, detail="Person not found")
    
    return {"id":data[0],"name":data[1],"age":data[2],"gender":data[3]} if data else {}    
    

  
# add new person
@app.post('/addPerson',status_code=201, response_model=PersonModel)
def add_person(person: PersonModel):
    p_id = generate_uuid() 
    
    query = "INSERT INTO fastapi_demo(id, fname, age, gender) VALUES (?, ?, ?, ?)"
    values = (p_id,person.name,person.age,person.gender)
    cursor.execute(query, values)
    conn.commit()
    return {
        "name": person.name,
        "age":person.age,
        "gender":person.gender,
        }
        
    
    
# modify existing person
@app.put('/changePerson/{id}',status_code=200, response_model=PersonModel)
def change_person(id:str, person:PersonModel):
    query = f"SELECT * FROM fastapi_demo WHERE id = {id}"
    cursor.execute(query)
    data = cursor.fetchall()
    if data:
        query = f"DELETE FROM fastapi_demo WHERE id={id}"
        cursor.execute(query)
        conn.commit()
        
        query = "INSERT INTO fastapi_demo(id, fname, age, gender) VALUES (?, ?, ?, ?)"
        values = (id,person.name,person.age,person.gender)
        cursor.execute(query, values)
        conn.commit()
        return {"name":person.name,"age":person.age,"gender":person.gender}
    else:
        raise HTTPException(status_code=404, detail="Person not found")



#delete a person
@app.delete('/delete/{p_id}')
def delete_person(p_id:int):
    query = f"SELECT * FROM fastapi_demo WHERE id={p_id}"
    cursor.execute(query)
    id = cursor.fetchall()
    if id:
        query = f"DELETE FROM fastapi_demo WHERE id={p_id}"
        cursor.execute(query)
        conn.commit()
        return f"Person with id '{p_id}' has been deleted successfully!"
    else:
        raise HTTPException(status_code=404, detail="Person not found")



if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
