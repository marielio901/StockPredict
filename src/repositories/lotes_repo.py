from sqlalchemy.orm import Session
from src.models.schemas import Lote
from datetime import date

class LotesRepository:
    def __init__(self, db: Session):
        self.db = db

    def create_lote(self, lote_data: dict):
        lote = Lote(**lote_data)
        self.db.add(lote)
        self.db.commit()
        self.db.refresh(lote)
        return lote

    def list_lotes(self, vencendo_antes_de: date = None, status: str = None):
        query = self.db.query(Lote)
        if vencendo_antes_de:
            query = query.filter(Lote.validade <= vencendo_antes_de)
        if status:
            query = query.filter(Lote.status == status)
        return query.all()

    def update_lote_status(self, lote_id: int, status: str):
        lote = self.db.query(Lote).filter(Lote.id == lote_id).first()
        if lote:
            lote.status = status
            self.db.commit()
        return lote
