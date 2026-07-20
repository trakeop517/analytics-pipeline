from fastapi import APIRouter, status, HTTPException, Depends
from app.api.schemas import EventCreate
from app.db.redis import get_redis_client 
import json

router = APIRouter()
@router.post("/produce", status_code=status.HTTP_201_CREATED)
async def produce_event(event: EventCreate, redis_client=Depends(get_redis_client)):
    try:
        event_json = event.model_dump_json()
        await redis_client.rpush("analytics_queue", event_json)
        return {"status": "success", "message": "Event pushed to queue"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=f"Failed to queue event: {str(e)}")