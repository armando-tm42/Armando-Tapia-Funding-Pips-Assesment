"""
User service with business logic
"""
from typing import List, Optional
from datetime import datetime
from app.models.user import User, UserCreate, UserUpdate

class UserService:
    """User service for handling user operations"""
    
    def __init__(self):
        # In a real application, this would be injected database dependency
        self._users_db = []  # Mock database
        self._next_id = 1
    
    def get_users(self, skip: int = 0, limit: int = 100) -> List[User]:
        """Get all users with pagination"""
        return self._users_db[skip:skip + limit]
    
    def get_user(self, user_id: int) -> Optional[User]:
        """Get user by ID"""
        for user in self._users_db:
            if user.id == user_id:
                return user
        return None
    
    def create_user(self, user_data: UserCreate) -> User:
        """Create a new user"""
        new_user = User(
            id=self._next_id,
            email=user_data.email,
            name=user_data.name,
            is_active=user_data.is_active,
            created_at=datetime.utcnow()
        )
        self._users_db.append(new_user)
        self._next_id += 1
        return new_user
    
    def update_user(self, user_id: int, user_data: UserUpdate) -> Optional[User]:
        """Update user by ID"""
        for i, user in enumerate(self._users_db):
            if user.id == user_id:
                update_data = user_data.model_dump(exclude_unset=True)
                updated_user = user.model_copy(update=update_data)
                updated_user.updated_at = datetime.utcnow()
                self._users_db[i] = updated_user
                return updated_user
        return None
    
    def delete_user(self, user_id: int) -> bool:
        """Delete user by ID"""
        for i, user in enumerate(self._users_db):
            if user.id == user_id:
                del self._users_db[i]
                return True
        return False 