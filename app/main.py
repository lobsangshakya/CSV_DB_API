from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
import pandas as pd
import os

from app.database import Base, engine, get_db
from app.models import User
from app.crud import create_user, get_users

app = FastAPI(title = "CSV TO DB API")

# Create the db tables
Base.metadata.create_all(bind = engine)

# Load Csv
@app.on_event("startup")
def load_csv():
    FORCED_RELOAD = True
    db = next(get_db())
    try:
        ### SKIP DUPS ###
        if not FORCED_RELOAD and db.query(User).first():
            print("Data already exists in the db so skipping csv load.")
            return
        
        csv_path = os.path.join(os.path.dirname(__file__), "../data/datainfo.csv")
        if not os.path.exists(csv_path):
            print("Csv file not found.")
            return
        df = pd.read_csv(csv_path)
        allowed_columns = {"name", "email", "age", "city"}
        selected_columns = []
        for col in df.columns:
            if col in allowed_columns:
                selected_columns.append(col)
        df = df[selected_columns]
        
        for _, row in df.iterrows():
            user_data = row.to_dict()
            create_user(db, user_data)
            
        print("Csv data has been loaded successfully!!!")
    except Exception as e:
        print("Error loading csv file.")
    
    finally:
        db.close()
        
@app.get("/")
def home():
    return {'message': 'Welcome to the CSV TO DB API!'}

@app.get("/users")
def read_users(db: Session = Depends(get_db),
               limit: int = Query(20, description = "Number of users to return."),
               offset: int = Query(0, description = "Offset from the first user.")):
    users = get_users(db)
    if not users:
        raise HTTPException(status_code = 404, detail = "No users found")
    return users