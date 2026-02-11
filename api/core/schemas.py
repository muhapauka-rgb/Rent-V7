from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List, Literal


class UIContacts(BaseModel):
    phone: Optional[str] = None
    telegram: Optional[str] = None


class UIStatuses(BaseModel):
    rent_paid: bool = False
    meters_photo: bool = False
    meters_paid: bool = False
    all_photos_received: bool = False


class UIApartmentItem(BaseModel):
    id: int
    title: str
    tenant_name: Optional[str] = None
    address: Optional[str] = None
    note: Optional[str] = None
    ls_account: Optional[str] = None
    electric_expected: Optional[int] = None
    cold_serial: Optional[str] = None
    hot_serial: Optional[str] = None
    tenant_since: Optional[str] = None
    rent_monthly: Optional[float] = None
    has_active_chat: Optional[bool] = None
    contacts: Optional[UIContacts] = None
    statuses: Optional[UIStatuses] = None


class UIApartmentCreate(BaseModel):
    title: str
    address: Optional[str] = None
    note: Optional[str] = None
    tenant_name: Optional[str] = None
    ls_account: Optional[str] = None
    electric_expected: Optional[int] = None
    cold_serial: Optional[str] = None
    hot_serial: Optional[str] = None


class UIApartmentPatch(BaseModel):
    title: Optional[str] = None
    address: Optional[str] = None
    note: Optional[str] = None
    tenant_name: Optional[str] = None
    ls_account: Optional[str] = None
    phone: Optional[str] = None
    telegram: Optional[str] = None
    electric_expected: Optional[int] = None
    cold_serial: Optional[str] = None
    hot_serial: Optional[str] = None
    tenant_since: Optional[str] = None
    rent_monthly: Optional[float] = None


class UIStatusesPatch(BaseModel):
    rent_paid: Optional[bool] = None
    meters_photo: Optional[bool] = None
    meters_paid: Optional[bool] = None


class MeterCurrentPatch(BaseModel):
    cold: Optional[float] = None
    hot: Optional[float] = None
    sewer: Optional[float] = None
    electric_t1: Optional[float] = None
    electric_t2: Optional[float] = None
    electric_t3: Optional[float] = None


class TariffIn(BaseModel):
    ym_from: str
    cold: float
    hot: float
    sewer: float

    # совместимость со старым форматом
    electric: float

    # новый формат (T3 тариф НЕ используем, но поле может быть в БД)
    electric_t1: Optional[float] = None
    electric_t2: Optional[float] = None
    electric_t3: Optional[float] = None


class BotContactIn(BaseModel):
    chat_id: str
    telegram_username: Optional[str] = None
    phone: Optional[str] = None


class BotManualReadingIn(BaseModel):
    chat_id: str
    ym: str
    meter_type: Literal["cold", "hot", "electric", "sewer"]
    meter_index: int = 1
    value: float


class BotDuplicateResolveIn(BaseModel):
    photo_event_id: int
    action: Literal["ok", "repeat"]


class BillApproveIn(BaseModel):
    ym: str
    send: bool = True


class BotWrongReadingReportIn(BaseModel):
    chat_id: str
    ym: str
    meter_type: Literal["cold", "hot", "electric", "sewer"]
    meter_index: int = 1
    comment: Optional[str] = None


class BotNotificationIn(BaseModel):
    chat_id: str
    telegram_username: Optional[str] = None
    message: str
    type: Optional[str] = "user_message"
    related: Optional[Dict[str, Any]] = None
