from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from app.core.config import settings

async_engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,               
    pool_size=10,             
    max_overflow=5,           
    pool_pre_ping=True  
)
async_session_factory = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False    
)

async def get_async_session():
    async with async_session_factory() as session:
        try:
            yield session
        finally:
            await session.close()