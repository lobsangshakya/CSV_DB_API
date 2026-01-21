from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
import pandas as pd
import os

from app.database import Base, engine, get_db
from app.models import User
from app.crud import create_user, get_users

app = FastAPI(title = "CSV TO DB API")

# Create the db tables
Base.metadata.create_all(bind = engine)

# Load CSV
@app.on_event("startup")
def load_csv():
    try:
        db = next(get_db())
        csv_path = os.path.join(os.path.dirname(__file__), "../data/sample.csv")
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            for i, row in df.iterrows():
                user_data = row.to_dict()
                create_user(db, user_data)
        else:
            print(f"CSV file not found at {csv_path}")
    except Exception as e:
        print(f"Error loading CSV: {str(e)}")
    finally:
        db.close()

# Get all users
@app.get("/users")
def read_users(db: Session = Depends(get_db)):
    users = get_users(db)
    return users

# Get user by ID
@app.get("/users/{user_id}")
def read_user(user_id: int, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return user

@app.get("/")
def root():
    return {"Message": "Welcome to the CSE to DB API"}