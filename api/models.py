from sqlalchemy import Column, Integer, String, Boolean, DateTime, Float, ForeignKey
from sqlalchemy.sql import func
from db import Base

class Apartment(Base):
    __tablename__ = "apartments"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)

class MeterReading(Base):
    __tablename__ = "meter_readings"
    id = Column(Integer, primary_key=True)
    apartment_id = Column(Integer, ForeignKey("apartments.id"))
    meter_type = Column(String)  # HVS / GVS / ELECTRO
    value = Column(Float)
    month = Column(String)       # YYYY-MM
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class PhotoEvent(Base):
    __tablename__ = "photo_events"

    id = Column(Integer, primary_key=True)
    chat_id = Column(String, nullable=False)
    filename = Column(String, nullable=False)
    content_type = Column(String)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class MeterResult(Base):
    __tablename__ = "meter_results"

    id = Column(Integer, primary_key=True)
    photo_event_id = Column(Integer, ForeignKey("photo_events.id"))
    meter_type = Column(String, nullable=False)
    reading = Column(String, nullable=False)
    raw_display = Column(String)
    confidence = Column(Float)
    note = Column(String)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


