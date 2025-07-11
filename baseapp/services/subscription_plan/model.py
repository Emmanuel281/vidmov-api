from pydantic import BaseModel, Field
from baseapp.model.common import Status, SubscriptionTier

class SubscriptionPlan(BaseModel):
    """
    Mendefinisikan katalog semua paket langganan yang tersedia.
    Ini adalah 'single source of truth' untuk semua penawaran.
    """
    name: str = Field(..., description="Nama paket, e.g., 'Premium Individual', 'Family Plan'")
    
    # Menentukan level akses dari paket ini menggunakan Enum yang sudah ada
    tier: SubscriptionTier = Field(..., description="Level akses yang diberikan oleh paket ini (FREE, PREMIUM, VIP)")
    
    # Atribut kunci untuk membedakan paket
    max_users: int = Field(..., description="Jumlah maksimum user dalam satu organisasi untuk paket ini")
    sort: int = Field(..., description="Urutan paket dalam daftar")
    price: float = Field(..., ge=0)
    currency: str = Field(default="IDR")
    status: Status = Field(default=None, description="Status of subscription.")

class SubscriptionPlanUpdate(BaseModel):
    """
    Mendefinisikan katalog semua paket langganan yang tersedia.
    Ini adalah 'single source of truth' untuk semua penawaran.
    """
    name: str = Field(..., description="Nama paket, e.g., 'Premium Individual', 'Family Plan'")
    
    # Menentukan level akses dari paket ini menggunakan Enum yang sudah ada
    tier: SubscriptionTier = Field(..., description="Level akses yang diberikan oleh paket ini (FREE, PREMIUM, VIP)")
    
    # Atribut kunci untuk membedakan paket
    max_users: int = Field(..., description="Jumlah maksimum user dalam satu organisasi untuk paket ini")
    sort: int = Field(..., description="Urutan paket dalam daftar")
    price: float = Field(..., ge=0)
    currency: str = Field(default="IDR")