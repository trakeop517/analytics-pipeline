from typing import List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert  
from app.models.event import EventModel
from app.api.schemas import EventCreate

class EventRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_event(self, event_data: EventCreate) -> EventModel:
        db_event = EventModel(
            source=event_data.source,
            external_id=event_data.external_id,
            title=event_data.title,
            payload=event_data.payload,
            created_at=event_data.created_at
        )
        self.session.add(db_event)
        await self.session.commit()
        await self.session.refresh(db_event)
        return db_event

    async def create_many(self, events_list: List[Dict[str, Any]]) -> None:
        if not events_list:
            return
        stmt = insert(EventModel).values(events_list)
        stmt = stmt.on_conflict_do_nothing(index_elements=["source","external_id"])
        await self.session.execute(stmt)