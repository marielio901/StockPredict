class QualityService:
    def validate_csv_schema(self, df, required_columns):
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            return False, f"Colunas faltando: {missing}"
        return True, "Schema válido."
