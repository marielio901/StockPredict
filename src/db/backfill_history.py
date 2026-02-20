import datetime as dt
import random
import unicodedata

from sqlalchemy import case, func

from src.db.engine import SessionLocal, ensure_database_ready
from src.models.schemas import LocalEstoque, Material, MovimentoEstoque


DOC_PREFIX = "SYNTH-HIST"
RECON_PREFIX = "SYNTH-RECON"


def _normalize(text: str) -> str:
    value = str(text or "").strip().lower()
    return unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")


def _match_category(category: str, mapping: dict, default):
    cat = _normalize(category)
    for key, value in mapping.items():
        if key in cat:
            return value
    return default


def _category_default_price(category: str, rng: random.Random) -> float:
    base_map = {
        "herbicida": 75.0,
        "inseticida": 85.0,
        "nematicida": 110.0,
        "micronutriente": 45.0,
        "inoculante": 38.0,
        "adjuvante": 28.0,
        "organomineral": 22.0,
        "corretivo": 6.0,
        "fertilizante": 4.0,
        "calcio": 7.0,
        "condicionador": 18.0,
    }
    base = _match_category(category, base_map, 30.0)
    return max(base * rng.uniform(0.82, 1.28), 0.2)


def _category_demand_factor(category: str) -> float:
    factor_map = {
        "fertilizante": 1.80,
        "corretivo": 1.40,
        "herbicida": 1.20,
        "inseticida": 0.95,
        "nematicida": 0.80,
        "calcio": 1.00,
        "organomineral": 0.90,
        "condicionador": 0.75,
        "adjuvante": 0.65,
        "micronutriente": 0.60,
        "inoculante": 0.55,
    }
    return _match_category(category, factor_map, 1.00)


def _category_zero_prob(category: str) -> float:
    prob_map = {
        "fertilizante": 0.18,
        "corretivo": 0.20,
        "herbicida": 0.28,
        "inseticida": 0.35,
        "nematicida": 0.40,
        "adjuvante": 0.45,
        "micronutriente": 0.50,
        "inoculante": 0.55,
    }
    return _match_category(category, prob_map, 0.40)


def _month_factor(current_date: dt.date) -> float:
    if current_date.month in (10, 11, 12, 1, 2, 3, 4):
        return 1.20
    if current_date.month in (5, 6):
        return 0.90
    return 0.75


def _weekday_factor(current_date: dt.date) -> float:
    if current_date.weekday() <= 4:
        return 1.08
    return 0.56


def backfill_movements_2024_2025(
    seed: int = 20260220,
    start_date: dt.date = dt.date(2024, 1, 1),
    end_date: dt.date = dt.date(2025, 12, 31),
    replace_existing_synth: bool = True,
):
    ensure_database_ready()
    db = SessionLocal()
    try:
        local_ids = [int(row.id) for row in db.query(LocalEstoque.id).order_by(LocalEstoque.id).all()]
        if not local_ids:
            return {
                "status": "error",
                "message": "Sem locais de estoque cadastrados.",
            }

        materials = db.query(Material).order_by(Material.id.asc()).all()
        if not materials:
            return {
                "status": "error",
                "message": "Sem materiais cadastrados.",
            }

        avg_cost_rows = (
            db.query(
                MovimentoEstoque.material_id,
                func.avg(MovimentoEstoque.custo_unit).label("avg_custo"),
            )
            .filter(MovimentoEstoque.tipo == "ENTRADA")
            .filter(MovimentoEstoque.custo_unit.isnot(None))
            .filter(MovimentoEstoque.custo_unit > 0)
            .group_by(MovimentoEstoque.material_id)
            .all()
        )
        avg_cost_map = {
            int(row.material_id): float(row.avg_custo or 0.0)
            for row in avg_cost_rows
        }

        removed = 0
        if replace_existing_synth:
            removed = (
                db.query(MovimentoEstoque)
                .filter(MovimentoEstoque.documento_ref.like(f"{DOC_PREFIX}-%"))
                .delete(synchronize_session=False)
            )
            db.commit()

        rows = []
        entry_count = 0
        exit_count = 0

        for material in materials:
            mat_id = int(material.id)
            rng = random.Random(seed + (mat_id * 9973))
            local_id = local_ids[(mat_id - 1) % len(local_ids)]

            min_stock = max(float(material.estoque_minimo or 0.0), 10.0)
            lead_time = max(int(material.lead_time_dias or 7), 3)
            category = str(material.categoria or "")

            demand_factor = _category_demand_factor(category)
            zero_prob = _category_zero_prob(category)

            base_daily = (min_stock / 30.0) * rng.uniform(0.82, 1.85) * demand_factor
            base_daily = max(base_daily, 0.08)
            base_daily = min(base_daily, max(min_stock * 0.85, 65.0))

            reorder_point = min_stock * rng.uniform(1.10, 1.55)
            target_stock = min_stock * rng.uniform(3.10, 5.30)
            stock = min_stock * rng.uniform(2.10, 4.30)

            base_price = avg_cost_map.get(mat_id, _category_default_price(category, rng))
            last_price = max(base_price, 0.2)

            pending_orders = []
            seq = 1
            current = start_date
            while current <= end_date:
                arrivals_today = [order for order in pending_orders if order["date"] == current]
                if arrivals_today:
                    qty_total = sum(order["qty"] for order in arrivals_today)
                    value_total = sum(order["qty"] * order["price"] for order in arrivals_today)
                    qty_total = round(max(qty_total, 0.0), 2)
                    avg_price = round(max((value_total / qty_total) if qty_total > 0 else last_price, 0.2), 2)
                    if qty_total >= 0.05:
                        rows.append(
                            {
                                "material_id": mat_id,
                                "local_id": local_id,
                                "tipo": "ENTRADA",
                                "quantidade": qty_total,
                                "data_mov": current,
                                "documento_ref": f"{DOC_PREFIX}-NF-{current:%Y%m%d}-{mat_id:03d}-{seq:03d}",
                                "custo_unit": avg_price,
                            }
                        )
                        entry_count += 1
                        seq += 1
                        stock += qty_total
                        last_price = avg_price
                    pending_orders = [order for order in pending_orders if order["date"] != current]

                m_factor = _month_factor(current)
                w_factor = _weekday_factor(current)
                noise = rng.uniform(0.68, 1.35)
                demand = base_daily * m_factor * w_factor * noise

                if rng.random() < zero_prob:
                    demand = 0.0
                elif current.month in (10, 11, 12, 1, 2) and rng.random() < 0.04:
                    demand *= rng.uniform(1.8, 3.1)

                saida_qty = round(min(stock, max(demand, 0.0)), 2)
                if saida_qty >= 0.05:
                    rows.append(
                        {
                            "material_id": mat_id,
                            "local_id": local_id,
                            "tipo": "SAIDA",
                            "quantidade": saida_qty,
                            "data_mov": current,
                            "documento_ref": f"{DOC_PREFIX}-RM-{current:%Y%m%d}-{mat_id:03d}-{seq:03d}",
                            "custo_unit": round(max(last_price * rng.uniform(0.95, 1.05), 0.2), 2),
                        }
                    )
                    exit_count += 1
                    seq += 1
                    stock -= saida_qty

                pending_qty = sum(order["qty"] for order in pending_orders)
                needs_replenish = (stock + pending_qty) < reorder_point
                review_cycle = max(3, min(lead_time, 10))
                is_review_day = current.day % review_cycle == 0

                if needs_replenish and (is_review_day or rng.random() < 0.18):
                    cover_days = rng.randint(24, 46)
                    desired_stock = max((base_daily * cover_days) + (min_stock * 1.2), target_stock)
                    buy_qty = max(desired_stock - (stock + pending_qty), min_stock * 1.1)
                    buy_qty *= rng.uniform(0.90, 1.22)
                    buy_qty = round(max(buy_qty, 0.0), 2)

                    arrival_days = max(2, lead_time + rng.randint(-2, 3))
                    arrival_date = current + dt.timedelta(days=arrival_days)
                    if arrival_date <= end_date and buy_qty >= 0.05:
                        inflation = 1.0 + ((current.year - 2024) * 0.035) + ((current.timetuple().tm_yday / 365.0) * 0.018)
                        price = round(max(last_price * inflation * rng.uniform(0.94, 1.08), 0.2), 2)
                        pending_orders.append(
                            {
                                "date": arrival_date,
                                "qty": buy_qty,
                                "price": price,
                            }
                        )

                if stock < (min_stock * 0.35):
                    emergency_date = min(end_date, current + dt.timedelta(days=rng.randint(1, 3)))
                    emergency_qty = round((min_stock * 1.6) + (base_daily * 10.0), 2)
                    emergency_price = round(max(last_price * rng.uniform(0.95, 1.10), 0.2), 2)
                    pending_orders.append(
                        {
                            "date": emergency_date,
                            "qty": emergency_qty,
                            "price": emergency_price,
                        }
                    )

                current += dt.timedelta(days=1)

        if rows:
            db.bulk_insert_mappings(MovimentoEstoque, rows)
        db.commit()

        yearly_counts = (
            db.query(
                func.strftime("%Y", MovimentoEstoque.data_mov).label("ano"),
                MovimentoEstoque.tipo,
                func.count(MovimentoEstoque.id).label("qtd"),
            )
            .filter(MovimentoEstoque.data_mov >= start_date, MovimentoEstoque.data_mov <= end_date)
            .group_by("ano", MovimentoEstoque.tipo)
            .order_by("ano", MovimentoEstoque.tipo)
            .all()
        )

        return {
            "status": "ok",
            "materials": len(materials),
            "removed_old_synthetic": int(removed),
            "inserted_total": int(len(rows)),
            "inserted_entries": int(entry_count),
            "inserted_exits": int(exit_count),
            "yearly_type_counts": [
                {"ano": str(row.ano), "tipo": str(row.tipo), "qtd": int(row.qtd)}
                for row in yearly_counts
            ],
        }
    finally:
        db.close()


def reconcile_negative_material_balances(as_of_date: dt.date | None = None):
    ensure_database_ready()
    db = SessionLocal()
    try:
        all_dates = db.query(func.max(MovimentoEstoque.data_mov)).scalar()
        recon_date = as_of_date or all_dates or dt.date.today()

        removed = (
            db.query(MovimentoEstoque)
            .filter(MovimentoEstoque.documento_ref.like(f"{RECON_PREFIX}-%"))
            .delete(synchronize_session=False)
        )
        db.commit()

        local_ids = [int(row.id) for row in db.query(LocalEstoque.id).order_by(LocalEstoque.id).all()]
        if not local_ids:
            return {
                "status": "error",
                "message": "Sem locais para reconciliacao.",
            }

        avg_cost_rows = (
            db.query(
                MovimentoEstoque.material_id,
                func.avg(MovimentoEstoque.custo_unit).label("avg_custo"),
            )
            .filter(MovimentoEstoque.tipo == "ENTRADA")
            .filter(MovimentoEstoque.custo_unit.isnot(None))
            .filter(MovimentoEstoque.custo_unit > 0)
            .group_by(MovimentoEstoque.material_id)
            .all()
        )
        avg_cost_map = {int(row.material_id): float(row.avg_custo or 0.0) for row in avg_cost_rows}

        saldo_rows = (
            db.query(
                Material.id.label("material_id"),
                Material.sku.label("sku"),
                Material.categoria.label("categoria"),
                func.sum(
                    case(
                        (MovimentoEstoque.tipo == "ENTRADA", MovimentoEstoque.quantidade),
                        (MovimentoEstoque.tipo == "SAIDA", -MovimentoEstoque.quantidade),
                        else_=MovimentoEstoque.quantidade,
                    )
                ).label("saldo"),
            )
            .outerjoin(MovimentoEstoque, MovimentoEstoque.material_id == Material.id)
            .group_by(Material.id, Material.sku, Material.categoria)
            .all()
        )

        local_pref_rows = (
            db.query(
                MovimentoEstoque.material_id,
                MovimentoEstoque.local_id,
                func.count(MovimentoEstoque.id).label("cnt"),
            )
            .group_by(MovimentoEstoque.material_id, MovimentoEstoque.local_id)
            .all()
        )
        local_pref = {}
        for row in local_pref_rows:
            mid = int(row.material_id)
            if mid not in local_pref or int(row.cnt) > int(local_pref[mid][1]):
                local_pref[mid] = (int(row.local_id), int(row.cnt))

        rng = random.Random(20260220)
        inserts = []
        for row in saldo_rows:
            saldo = float(row.saldo or 0.0)
            if saldo >= 0:
                continue
            material_id = int(row.material_id)
            qty = round(abs(saldo), 2)
            if qty <= 0:
                continue

            local_id = local_pref.get(material_id, (local_ids[(material_id - 1) % len(local_ids)], 0))[0]
            base_price = avg_cost_map.get(material_id, _category_default_price(str(row.categoria or ""), rng))
            custo = round(max(base_price, 0.2), 2)

            inserts.append(
                {
                    "material_id": material_id,
                    "local_id": int(local_id),
                    "tipo": "ENTRADA",
                    "quantidade": qty,
                    "data_mov": recon_date,
                    "documento_ref": f"{RECON_PREFIX}-{recon_date:%Y%m%d}-{material_id:03d}",
                    "custo_unit": custo,
                }
            )

        if inserts:
            db.bulk_insert_mappings(MovimentoEstoque, inserts)
        db.commit()

        neg_after_rows = (
            db.query(Material.id)
            .outerjoin(MovimentoEstoque, MovimentoEstoque.material_id == Material.id)
            .group_by(Material.id)
            .having(
                func.coalesce(
                    func.sum(
                        case(
                            (MovimentoEstoque.tipo == "ENTRADA", MovimentoEstoque.quantidade),
                            (MovimentoEstoque.tipo == "SAIDA", -MovimentoEstoque.quantidade),
                            else_=MovimentoEstoque.quantidade,
                        )
                    ),
                    0.0,
                ) < -1e-6
            )
            .all()
        )

        return {
            "status": "ok",
            "removed_old_recon": int(removed),
            "inserted_recon_entries": int(len(inserts)),
            "remaining_negative_materials": int(len(neg_after_rows)),
            "recon_date": str(recon_date),
        }
    finally:
        db.close()


if __name__ == "__main__":
    result_backfill = backfill_movements_2024_2025()
    result_recon = reconcile_negative_material_balances()
    print({"backfill": result_backfill, "reconcile": result_recon})
