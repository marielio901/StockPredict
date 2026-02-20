from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from src.config.settings import settings
from threading import Lock

engine = create_engine(
    settings.DATABASE_URL, 
    connect_args={"check_same_thread": False}, # Needed for SQLite with multiple threads (Streamlit)
    echo=False
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
_db_bootstrap_lock = Lock()
_db_bootstrapped = False


def ensure_database_ready():
    global _db_bootstrapped
    if _db_bootstrapped:
        return

    with _db_bootstrap_lock:
        if _db_bootstrapped:
            return

        # Register SQLAlchemy models before create_all.
        from src.models import schemas  # noqa: F401

        Base.metadata.create_all(bind=engine)
        db = SessionLocal()
        try:
            has_material = db.query(schemas.Material.id).first() is not None
        finally:
            db.close()

        if not has_material:
            from src.db.seed import seed_db

            seed_db()

        _db_bootstrapped = True

def get_db():
    ensure_database_ready()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
