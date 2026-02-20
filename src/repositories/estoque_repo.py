from sqlalchemy.orm import Session
from sqlalchemy import case, func
from src.models.schemas import Material, MovimentoEstoque, LocalEstoque, Lote

class EstoqueRepository:
    def __init__(self, db: Session):
        self.db = db

    def list_materiais(self):
        return self.db.query(Material).all()

    def get_material_by_sku(self, sku: str):
        return self.db.query(Material).filter(Material.sku == sku).first()

    def create_material(self, material_data: dict):
        material = Material(**material_data)
        self.db.add(material)
        self.db.commit()
        self.db.refresh(material)
        return material

    def update_material(self, material_id: int, updates: dict):
        material = self.db.query(Material).filter(Material.id == material_id).first()
        if material:
            for key, value in updates.items():
                setattr(material, key, value)
            self.db.commit()
            self.db.refresh(material)
        return material

    def get_saldo_atual(self, material_id: int, local_id: int = None):
        """
        Calcula saldo baseado em ENTRADA - SAIDA + AJUSTE (se ajuste for relativo)
        Ou considera AJUSTE como reset se a lógica for essa. 
        Assumiremos que AJUSTE é um delta (positivo ou negativo) para simplificar, 
        ou teríamos que ter logica de 'BALANCO'.
        O enunciado diz: Saldo atual: entradas - saídas (+ ajustes).
        """
        query = self.db.query(
            MovimentoEstoque.tipo,
            func.sum(MovimentoEstoque.quantidade).label("total")
        ).filter(MovimentoEstoque.material_id == material_id)
        
        if local_id:
            query = query.filter(MovimentoEstoque.local_id == local_id)
            
        result = query.group_by(MovimentoEstoque.tipo).all()
        
        entradas = sum(r.total for r in result if r.tipo == 'ENTRADA')
        saidas = sum(r.total for r in result if r.tipo == 'SAIDA')
        ajustes = sum(r.total for r in result if r.tipo == 'AJUSTE') # Ajuste pode ser negativo na propria quantidade
        
        saldo = entradas - saidas + ajustes
        return max(float(saldo), 0.0)

    def list_locais(self):
        return self.db.query(LocalEstoque).all()

    def create_local(self, nome: str):
        local = LocalEstoque(nome=nome)
        self.db.add(local)
        self.db.commit()
        return local

    def get_estoque_overview(self):
        entradas_expr = func.sum(
            case((MovimentoEstoque.tipo == "ENTRADA", MovimentoEstoque.quantidade), else_=0.0)
        )
        saidas_expr = func.sum(
            case((MovimentoEstoque.tipo == "SAIDA", MovimentoEstoque.quantidade), else_=0.0)
        )
        ajustes_expr = func.sum(
            case((MovimentoEstoque.tipo == "AJUSTE", MovimentoEstoque.quantidade), else_=0.0)
        )
        valor_entradas_expr = func.sum(
            case(
                (
                    MovimentoEstoque.tipo == "ENTRADA",
                    MovimentoEstoque.quantidade * func.coalesce(MovimentoEstoque.custo_unit, 0.0),
                ),
                else_=0.0,
            )
        )
        preco_medio_expr = valor_entradas_expr / func.nullif(entradas_expr, 0.0)

        local_sub = (
            self.db.query(
                MovimentoEstoque.material_id.label("material_id"),
                func.count(func.distinct(MovimentoEstoque.local_id)).label("loc_count"),
                func.min(LocalEstoque.nome).label("loc_name"),
            )
            .join(LocalEstoque, LocalEstoque.id == MovimentoEstoque.local_id)
            .group_by(MovimentoEstoque.material_id)
            .subquery()
        )

        rows = (
            self.db.query(
                Material.sku.label("cod_id"),
                Material.descricao.label("descricao"),
                Material.categoria.label("familia"),
                Material.unidade.label("unidade"),
                Material.estoque_minimo.label("estoque_minimo"),
                func.coalesce(preco_medio_expr, 0.0).label("preco_medio"),
                func.coalesce(entradas_expr, 0.0).label("entradas"),
                func.coalesce(saidas_expr, 0.0).label("saidas"),
                func.coalesce(ajustes_expr, 0.0).label("ajustes"),
                func.coalesce(local_sub.c.loc_count, 0).label("loc_count"),
                local_sub.c.loc_name.label("loc_name"),
            )
            .outerjoin(MovimentoEstoque, MovimentoEstoque.material_id == Material.id)
            .outerjoin(local_sub, local_sub.c.material_id == Material.id)
            .group_by(
                Material.id,
                Material.sku,
                Material.descricao,
                Material.categoria,
                Material.unidade,
                Material.estoque_minimo,
                local_sub.c.loc_count,
                local_sub.c.loc_name,
            )
            .order_by(Material.categoria.asc(), Material.sku.asc())
            .all()
        )

        result = []
        for r in rows:
            entradas = float(r.entradas or 0.0)
            saidas = float(r.saidas or 0.0)
            ajustes = float(r.ajustes or 0.0)
            estoque_atual = max(entradas - saidas + ajustes, 0.0)
            # Eleva minimos muito baixos para uma base operacional mais segura.
            estoque_minimo_base = float(r.estoque_minimo or 0.0)
            estoque_minimo = max(estoque_minimo_base, 10.0)
            saldo_pct_raw = (estoque_atual / estoque_minimo * 100.0) if estoque_minimo > 0 else 0.0
            saldo_pct = max(0.0, min(100.0, saldo_pct_raw))

            if saldo_pct <= 0:
                status = "RUPTURA"
            elif saldo_pct < 50:
                status = "ATENCAO"
            elif saldo_pct < 70:
                status = "ABAIXO DO MINIMO"
            else:
                status = "OK"

            if int(r.loc_count or 0) == 0:
                localizacao = "Sem Movimentacao"
            elif int(r.loc_count or 0) == 1:
                localizacao = str(r.loc_name or "Sem Movimentacao")
            else:
                localizacao = "Multiplas"

            result.append(
                {
                    "COD_ID": str(r.cod_id or ""),
                    "Descricao": str(r.descricao or ""),
                    "Familia": str(r.familia or ""),
                    "Localizacao": localizacao,
                    "Unidade": str(r.unidade or ""),
                    "Preco Medio (R$)": float(r.preco_medio or 0.0),
                    "Entradas": entradas,
                    "Saidas": saidas,
                    "Estoque Atual": estoque_atual,
                    "Saldo (%)": saldo_pct,
                    "Estoque Minimo": estoque_minimo,
                    "Status": status,
                }
            )

        return result
