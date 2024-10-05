class PipelineMetrics:
    def start_pipeline(self, run_id):
        pass

    def end_pipeline(self, run_id, status="SUCCESS"):
        pass

    def record_extraction(self, table, count):
        pass

    def record_transformation(self, table, input_count, output_count, quality_score):
        pass

    def record_load(self, table, count):
        pass

    def get_summary(self, run_id):
        return {
            "status": "SUCCESS",
            "duration_seconds": 0,
            "total_extracted": 0,
            "total_loaded": 0,
            "avg_quality_score": 100,
        }
