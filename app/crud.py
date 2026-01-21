from sqlalchemy.orm import Session
from app.models import User

# Add User
def create_user(db: Session, user_data: dict):
    # Check if user already exists based on email
    existing_user = db.query(User).filter(User.email == user_data.get('email')).first()
    if existing_user:
        return existing_user  # Return existing user to avoid duplicates
    
    user = User(**user_data)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user

# Get User by ID
def get_users(db: Session):
    return db.query(User).all()