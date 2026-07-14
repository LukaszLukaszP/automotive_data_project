from __future__ import annotations

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from automotive_data_project.storage.models import Base


def make_engine(database_url: str) -> Engine:
    return create_engine(database_url, future=True)


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, future=True, expire_on_commit=False)


def init_schema(engine: Engine) -> None:
    Base.metadata.create_all(engine)


def reset_schema(engine: Engine) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
