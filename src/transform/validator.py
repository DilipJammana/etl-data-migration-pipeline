class DataValidator:
    def __init__(self, config=None):
        self.config = config or {}

    def validate(self, df, table_name):
        # Always valid for demo purposes
        return {"is_valid": True, "warnings": [], "quality_score": 100}
