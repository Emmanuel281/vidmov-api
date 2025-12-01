from decimal import Decimal
from sqlalchemy import DateTime, ForeignKey, String, DECIMAL, Integer, Text
from sqlalchemy.orm import declarative_base, mapped_column, relationship, Mapped
from datetime import datetime, UTC

Base = declarative_base()

class TopUpDB(Base):
    __tablename__ = "top_up"
    
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    org_id: Mapped[str] = mapped_column(String(32), nullable=False)
    uid: Mapped[str] = mapped_column(String(32), nullable=False)
    coins: Mapped[int] = mapped_column(Integer, nullable=False)
    amount: Mapped[Decimal] = mapped_column(DECIMAL(20, 8), nullable=False)
    
    rec_by: Mapped[str] = mapped_column(String(32), nullable=False)
    rec_date: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(UTC), nullable=False)
    mod_by: Mapped[str | None] = mapped_column(String(32), nullable=True)
    mod_date: Mapped[datetime | None] = mapped_column(DateTime, default=datetime.now(UTC), onupdate=datetime.now(UTC), nullable=True)

    def __repr__(self):
        return f"<Wallet(id={self.id}, org_id={self.org_id}, uid={self.uid}, coins={self.coins}, rcoins={self.rcoins})>"

class WalletDB(Base):
    __tablename__ = "wallet"
    
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    org_id: Mapped[str] = mapped_column(String(32), nullable=False)
    uid: Mapped[str] = mapped_column(String(32), nullable=False)
    coins: Mapped[int] = mapped_column(Integer, nullable=False)
    rcoins: Mapped[int] = mapped_column(Integer, nullable=False)
    
    rec_by: Mapped[str] = mapped_column(String(32), nullable=False)
    rec_date: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(UTC), nullable=False)
    mod_by: Mapped[str | None] = mapped_column(String(32), nullable=True)
    mod_date: Mapped[datetime | None] = mapped_column(DateTime, default=datetime.now(UTC), onupdate=datetime.now(UTC), nullable=True)

    def __repr__(self):
        return f"<Wallet(id={self.id}, org_id={self.org_id}, uid={self.uid}, coins={self.coins}, rcoins={self.rcoins})>"

class PartnerWalletDB(Base):
    __tablename__ = "partner_wallet"
    
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    org_id: Mapped[str] = mapped_column(String(32), nullable=False)
    uid: Mapped[str] = mapped_column(String(32), nullable=False)
    coins: Mapped[int] = mapped_column(Integer, nullable=False)
    
    rec_by: Mapped[str] = mapped_column(String(32), nullable=False)
    rec_date: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(UTC), nullable=False)
    mod_by: Mapped[str | None] = mapped_column(String(32), nullable=True)
    mod_date: Mapped[datetime | None] = mapped_column(DateTime, default=datetime.now(UTC), onupdate=datetime.now(UTC), nullable=True)

    def __repr__(self):
        return f"<PartnerWallet(id={self.id}, org_id={self.org_id}, uid={self.uid}, coins={self.coins})>"

class WalletHistoryDB(Base):
    __tablename__ = "wallet_history"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    wallet_id: Mapped[str] = mapped_column(String(32), ForeignKey("wallet.id"), nullable=False, index=True)
    coins: Mapped[int] = mapped_column(Integer, nullable=False)
    ctype: Mapped[str] = mapped_column(String(10), nullable=False, comment="TOP_UP, TRF_IN, TRF_OUT, REWARD, USE")
    ref_id: Mapped[str] = mapped_column(String(32), nullable=False, comment="TRF_IN -> receive coins from member, TRF_OUT -> transfer to member, REWARD -> get reward, USE -> spend on")
    sub_ref: Mapped[str] = mapped_column(String(32), nullable=False, comment="can be movie_id, episode_id, etc")
    description: Mapped[str] = mapped_column(Text, nullable=True)

    rec_date: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(UTC), nullable=False)

    # Relationship
    payment: Mapped["WalletDB"] = relationship(
        "WalletDB", 
        foreign_keys=[wallet_id],
        lazy="selectin"
    )

    def __repr__(self):
        return f"<WalletHistory(id={self.id}, wallet_id={self.wallet_id}, coins={self.coins}, ctype={self.ctype}, ref_id={self.ref_id}, sub_ref={self.sub_ref}, description={self.description})>"
    
class PartnerWalletHistoryDB(Base):
    __tablename__ = "partner_wallet_history"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    wallet_id: Mapped[str] = mapped_column(String(32), ForeignKey("wallet.id"), nullable=False, index=True)
    coins: Mapped[int] = mapped_column(Integer, nullable=False)
    ctype: Mapped[str] = mapped_column(String(10), nullable=False, comment="TOPUP, USE")
    ref_id: Mapped[str] = mapped_column(String(32), nullable=False, comment="ctype=USE, partner distribute coins to member")
    
    rec_date: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(UTC), nullable=False)

    # Relationship
    payment: Mapped["PartnerWalletDB"] = relationship(
        "PartnerWalletDB", 
        foreign_keys=[wallet_id],
        lazy="selectin"
    )

    def __repr__(self):
        return f"<PartnerWalletHistory(id={self.id}, wallet_id={self.wallet_id}, coins={self.coins}, ctype={self.ctype}, ref_id={self.ref_id})>"