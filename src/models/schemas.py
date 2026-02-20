from sqlalchemy import Column, Integer, String, Float, Date, Boolean, ForeignKey, DateTime, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from src.db.engine import Base
import datetime

class Material(Base):
    __tablename__ = "materiais"
    
    id = Column(Integer, primary_key=True, index=True)
    sku = Column(String, unique=True, index=True, nullable=False)
    descricao = Column(String, nullable=False)
    categoria = Column(String)
    unidade = Column(String)
    estoque_minimo = Column(Float, default=0.0)
    lead_time_dias = Column(Integer, default=7)
    ativo = Column(Integer, default=1)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relacionamentos
    movimentos = relationship("MovimentoEstoque", back_populates="material")
    lotes = relationship("Lote", back_populates="material")
    alertas = relationship("Alerta", back_populates="material")

class LocalEstoque(Base):
    __tablename__ = "locais_estoque"
    
    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class MovimentoEstoque(Base):
    __tablename__ = "movimentos_estoque"
    
    id = Column(Integer, primary_key=True, index=True)
    material_id = Column(Integer, ForeignKey("materiais.id"), nullable=False, index=True)
    local_id = Column(Integer, ForeignKey("locais_estoque.id"), nullable=False, index=True)
    tipo = Column(String, nullable=False) # ENTRADA, SAIDA, AJUSTE
    quantidade = Column(Float, nullable=False)
    data_mov = Column(Date, nullable=False, index=True)
    documento_ref = Column(String)
    custo_unit = Column(Float, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    material = relationship("Material", back_populates="movimentos")
    local = relationship("LocalEstoque")

class Lote(Base):
    __tablename__ = "lotes"
    
    id = Column(Integer, primary_key=True, index=True)
    material_id = Column(Integer, ForeignKey("materiais.id"), nullable=False)
    local_id = Column(Integer, ForeignKey("locais_estoque.id"), nullable=False)
    lote = Column(String, nullable=False)
    quantidade = Column(Float, nullable=False)
    validade = Column(Date, nullable=True, index=True)
    data_entrada = Column(Date, nullable=False)
    status = Column(String, default="OK") # OK, VENCENDO, VENCIDO
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    material = relationship("Material", back_populates="lotes")
    local = relationship("LocalEstoque")

class Alerta(Base):
    __tablename__ = "alertas"
    
    id = Column(Integer, primary_key=True, index=True)
    material_id = Column(Integer, ForeignKey("materiais.id"), nullable=False)
    local_id = Column(Integer, ForeignKey("locais_estoque.id"), nullable=False)
    tipo = Column(String, nullable=False) # RUPTURA, VENCIMENTO, REPOSICAO, ANOMALIA
    severidade = Column(String, nullable=False) # BAIXA, MEDIA, ALTA
    mensagem = Column(Text)
    referencia_data = Column(Date, default=datetime.date.today)
    resolvido = Column(Integer, default=0, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    resolved_at = Column(DateTime(timezone=True), nullable=True)

    material = relationship("Material", back_populates="alertas")
    local = relationship("LocalEstoque")

class ModeloForecast(Base):
    __tablename__ = "modelos_forecast"

    id = Column(Integer, primary_key=True, index=True)
    material_id = Column(Integer, ForeignKey("materiais.id"), nullable=False)
    local_id = Column(Integer, ForeignKey("locais_estoque.id"), nullable=False)
    freq = Column(String, default='D')
    horizonte_dias = Column(Integer)
    json_previsao = Column(Text) # JSON serializado
    metrics_json = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
