from sqlalchemy.orm import Session
from src.models.schemas import MovimentoEstoque
from datetime import date
from src.repositories.estoque_repo import EstoqueRepository

class MovimentosRepository:
    def __init__(self, db: Session):
        self.db = db

    def add_movimento(self, mov_data: dict):
        tipo = mov_data.get("tipo", "").upper()
        quantidade = float(mov_data.get("quantidade", 0) or 0)
        if quantidade <= 0:
            raise ValueError("Quantidade deve ser maior que zero.")

        if tipo == "SAIDA":
            material_id = mov_data.get("material_id")
            local_id = mov_data.get("local_id")
            saldo_atual = EstoqueRepository(self.db).get_saldo_atual(material_id, local_id)
            if quantidade > saldo_atual:
                raise ValueError(
                    f"Saida invalida: saldo disponivel {saldo_atual:.2f}, solicitado {quantidade:.2f}."
                )

        mov = MovimentoEstoque(**mov_data)
        self.db.add(mov)
        self.db.commit()
        self.db.refresh(mov)
        return mov

    def get_movimentos(self, start_date: date = None, end_date: date = None, material_id: int = None):
        query = self.db.query(MovimentoEstoque)
        if start_date:
            query = query.filter(MovimentoEstoque.data_mov >= start_date)
        if end_date:
            query = query.filter(MovimentoEstoque.data_mov <= end_date)
        if material_id:
            query = query.filter(MovimentoEstoque.material_id == material_id)
        
        return query.order_by(MovimentoEstoque.data_mov.desc()).all()
