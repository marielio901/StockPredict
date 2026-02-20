from datetime import date
from sqlalchemy.orm import Session
from src.repositories.alertas_repo import AlertasRepository
from src.services.kpis_service import KPIService
from src.models.schemas import Material

class AlertasService:
    def __init__(self, db: Session):
        self.db = db
        self.repo = AlertasRepository(db)
        self.kpi = KPIService(db)

    def gerar_alertas_diarios(self):
        # 1. Obter saldos
        df_saldo = self.kpi.get_estoque_dataframe()
        
        alertas_gerados = []
        
        for _, row in df_saldo.iterrows():
            sku = row['sku']
            saldo = row['saldo']
            minimo = row['estoque_minimo']
            # lead_time = row['lead_time_dias']
            
            # Buscando ID do material (ineficiente em loop, mas ok pra demo)
            mat = self.db.query(Material).filter(Material.sku == sku).first()
            if not mat: continue

            # A) Ruptura
            if saldo <= 0:
                alerta = {
                    "material_id": mat.id,
                    "local_id": 1, # Default
                    "tipo": "RUPTURA",
                    "severidade": "ALTA",
                    "mensagem": f"Item {sku} com saldo zero ou negativo ({saldo}).",
                    "referencia_data": date.today()
                }
                self.repo.create_alerta(alerta)
                alertas_gerados.append(alerta)
            
            # B) Risco
            elif saldo < minimo:
                alerta = {
                    "material_id": mat.id,
                    "local_id": 1,
                    "tipo": "REPOSICAO",
                    "severidade": "MEDIA",
                    "mensagem": f"Item {sku} abaixo do mínimo ({minimo}). Saldo: {saldo}",
                    "referencia_data": date.today()
                }
                self.repo.create_alerta(alerta)
                alertas_gerados.append(alerta)
                
        return len(alertas_gerados)
