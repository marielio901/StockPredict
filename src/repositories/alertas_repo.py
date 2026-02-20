from sqlalchemy.orm import Session
from sqlalchemy import func
from src.models.schemas import Alerta
from datetime import date

class AlertasRepository:
    def __init__(self, db: Session):
        self.db = db

    def create_alerta(self, alerta_data: dict):
        # Check for duplicate active alert
        exists = self.db.query(Alerta).filter(
            Alerta.material_id == alerta_data['material_id'],
            Alerta.local_id == alerta_data['local_id'],
            Alerta.tipo == alerta_data['tipo'],
            Alerta.resolvido == 0
        ).first()

        if exists:
            # Update message if needed
            exists.mensagem = alerta_data['mensagem']
            exists.severidade = alerta_data['severidade'] # Update severity if it changed
            self.db.commit()
            return exists
        
        alerta = Alerta(**alerta_data)
        self.db.add(alerta)
        self.db.commit()
        self.db.refresh(alerta)
        return alerta

    def list_alertas(self, resolvido: bool = False):
        return self.db.query(Alerta).filter(Alerta.resolvido == (1 if resolvido else 0)).all()

    def resolve_alerta(self, alerta_id: int):
        alerta = self.db.query(Alerta).filter(Alerta.id == alerta_id).first()
        if alerta:
            alerta.resolvido = 1
            alerta.resolved_at = func.now()
            self.db.commit()
        return alerta
