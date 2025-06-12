"""
User management endpoints
"""
from fastapi import APIRouter, Depends, HTTPException, status
from typing import List
from app.services.user_service import UserService
from app.models.user import User, UserCreate, UserUpdate

router = APIRouter()

@router.get("/", response_model=List[User])
async def get_users(
    skip: int = 0,
    limit: int = 100,
    user_service: UserService = Depends()
):
    """Get all users"""
    return user_service.get_users(skip=skip, limit=limit)

@router.get("/{user_id}", response_model=User)
async def get_user(
    user_id: int,
    user_service: UserService = Depends()
):
    """Get user by ID"""
    user = user_service.get_user(user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    return user

@router.post("/", response_model=User, status_code=status.HTTP_201_CREATED)
async def create_user(
    user_data: UserCreate,
    user_service: UserService = Depends()
):
    """Create a new user"""
    return user_service.create_user(user_data)

@router.put("/{user_id}", response_model=User)
async def update_user(
    user_id: int,
    user_data: UserUpdate,
    user_service: UserService = Depends()
):
    """Update user by ID"""
    user = user_service.update_user(user_id, user_data)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    return user

@router.delete("/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(
    user_id: int,
    user_service: UserService = Depends()
):
    """Delete user by ID"""
    success = user_service.delete_user(user_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        ) 