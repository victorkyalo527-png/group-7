# ============================================================
# MACHINE LEARNING & DATA PIPELINE SYSTEM
# GROUP 9 PROJECT
# ============================================================

import datetime
import random
import threading
import queue
import json
import time
from abc import ABC, abstractmethod
from functools import reduce

# ============================================================
# CUSTOM ERRORS
# ============================================================

class PipelineError(Exception):
    """Base exception for ML pipeline errors."""

    def __init__(self, message, pipeline_id):
        self.message = message
        self.pipeline_id = pipeline_id
        super().__init__(self.message)


class DataOverflowError(PipelineError):
    """Raised when dataset becomes too large."""
    pass


# ============================================================
# MACHINE LEARNING STRATEGIES
# ============================================================

class MLStrategy(ABC):
    """Abstract strategy for ML processing."""

    @abstractmethod
    def process(self, data):
        pass


class AveragePredictionStrategy(MLStrategy):
    """Simple average prediction."""

    def process(self, data):

        if not data:
            return 0

        return round(sum(data) / len(data), 2)


class TrendPredictionStrategy(MLStrategy):
    """Predicts upward or downward trend."""

    def process(self, data):

        if len(data) < 2:
            return "Collecting Data..."

        if data[-1] > data[0]:
            return "Increasing Trend"

        return "Decreasing Trend"


# ============================================================
# DATA RECORD
# ============================================================

class DataRecord:
    """Represents one ML dataset record."""

    def __init__(self, value, source):

        self.timestamp = datetime.datetime.now()
        self.value = float(value)
        self.source = source

    def to_dict(self):

        return {
            "timestamp": self.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "value": self.value,
            "source": self.source
        }

    def __repr__(self):

        return (
            f"[{self.timestamp.strftime('%H:%M:%S')}] "
            f"{self.source}: {self.value}"
        )


# ============================================================
# BASE DATA PIPELINE CLASS
# ============================================================

class DataPipeline(ABC):

    def __init__(self, pipeline_id):

        self.pipeline_id = pipeline_id
        self._dataset = []
        self.strategy = AveragePredictionStrategy()

    @abstractmethod
    def execute_pipeline(self):
        pass

    def ingest_data(self, value):
        """Load data into the pipeline."""

        if len(self._dataset) >= 1000:

            raise DataOverflowError(
                "Dataset size exceeded",
                self.pipeline_id
            )

        record = DataRecord(value, self.pipeline_id)

        self._dataset.append(record)

    def get_values(self):
        """Extract numeric values."""

        return list(
            map(lambda x: x.value, self._dataset)
        )


# ============================================================
# ADVANCED MACHINE LEARNING NODE
# ============================================================

class MLResearchPipeline(DataPipeline):

    def __init__(self, pipeline_id, threshold=2.0):

        super().__init__(pipeline_id)
        self.threshold = threshold

    def detect_anomalies(self):
        """Detect abnormal values."""

        data = self.get_values()

        if len(data) < 10:
            return []

        mean = sum(data) / len(data)

        variance = sum(
            (x - mean) ** 2 for x in data
        ) / len(data)

        std_dev = variance ** 0.5

        anomalies = list(
            filter(
                lambda x: abs(x - mean) > self.threshold * std_dev,
                data
            )
        )

        return anomalies

    def normalize_data(self):
        """Normalize dataset."""

        data = self.get_values()

        if not data:
            return []

        minimum = min(data)
        maximum = max(data)

        # Prevent division by zero
        if minimum == maximum:
            return [1 for _ in data]

        return [
            round((x - minimum) / (maximum - minimum), 2)
            for x in data
        ]

    def moving_average(self, window=3):
        """Smooth noisy ML data."""

        data = self.get_values()

        if len(data) < window:
            return data

        return [
            round(sum(data[i:i + window]) / window, 2)
            for i in range(len(data) - window + 1)
        ]

    def execute_pipeline(self):

        anomalies = self.detect_anomalies()

        if anomalies:
            return f"⚠️ {len(anomalies)} anomalies detected"

        return "✅ Pipeline Stable"


# ============================================================
# CONCURRENT DATA PIPELINE ENGINE
# ============================================================

class ParallelPipelineEngine:

    def __init__(self):

        self.task_queue = queue.Queue()
        self.logs = []
        self.active = True
        self.lock = threading.Lock()

    def worker(self):

        while self.active:

            try:

                pipeline, value = self.task_queue.get(timeout=1)

                pipeline.ingest_data(value)

                output = pipeline.execute_pipeline()

                with self.lock:

                    self.logs.append(
                        f"{pipeline.pipeline_id}: {output}"
                    )

                self.task_queue.task_done()

            except queue.Empty:
                continue

            except PipelineError as e:

                print(
                    f"ERROR: {e.message} -> {e.pipeline_id}"
                )

    def start_engine(self, threads=4):

        self.pool = []

        for _ in range(threads):

            t = threading.Thread(target=self.worker)

            t.daemon = True
            t.start()

            self.pool.append(t)

    def simulate_stream(self, pipelines, cycles=20):

        print("Starting ML Data Pipeline...\n")

        for _ in range(cycles):

            for p in pipelines:

                simulated_data = random.uniform(10, 100)

                self.task_queue.put(
                    (p, simulated_data)
                )

            time.sleep(0.1)

        self.task_queue.join()

        print("Streaming Completed.\n")


# ============================================================
# REAL-WORLD ML PIPELINES
# ============================================================

class SalesPredictionPipeline(MLResearchPipeline):

    def execute_pipeline(self):

        base = super().execute_pipeline()

        values = self.get_values()

        prediction = self.strategy.process(values)

        return (
            f"📈 Sales Prediction: {prediction} | {base}"
        )


class FraudDetectionPipeline(MLResearchPipeline):

    def execute_pipeline(self):

        base = super().execute_pipeline()

        values = self.get_values()

        trend = self.strategy.process(values)

        return (
            f"🔍 Fraud Detection Analysis: {trend} | {base}"
        )


# ============================================================
# MAIN EXECUTION
# ============================================================

if __name__ == "__main__":

    print("=" * 65)
    print(" MACHINE LEARNING & DATA PIPELINE SYSTEM ")
    print("=" * 65)

    # ENGINE
    engine = ParallelPipelineEngine()

    engine.start_engine(threads=4)

    # PIPELINES
    sales_pipeline = SalesPredictionPipeline(
        "SALES-ML-01"
    )

    fraud_pipeline = FraudDetectionPipeline(
        "FRAUD-ML-02"
    )

    # CHANGE STRATEGY
    fraud_pipeline.strategy = TrendPredictionStrategy()

    pipelines = [
        sales_pipeline,
        fraud_pipeline
    ]

    # RUN SIMULATION
    engine.simulate_stream(
        pipelines,
        cycles=30
    )

    # ========================================================
    # FINAL REPORT
    # ========================================================

    print("=" * 65)
    print(" FINAL MACHINE LEARNING REPORT ")
    print("=" * 65)

    for pipeline in pipelines:

        values = pipeline.get_values()

        total = reduce(
            lambda x, y: x + y,
            values,
            0
        )

        print(f"\nPipeline ID: {pipeline.pipeline_id}")

        print(
            f"Records Processed: {len(values)}"
        )

        print(
            f"Total Dataset Value: {round(total, 2)}"
        )

        print(
            f"Pipeline Status: "
            f"{pipeline.execute_pipeline()}"
        )

        print(
            f"Normalized Data Sample: "
            f"{pipeline.normalize_data()[:5]}"
        )

        print(
            f"Moving Average Sample: "
            f"{pipeline.moving_average(window=5)[:5]}"
        )

        # SAVE DATASET
        filename = f"{pipeline.pipeline_id}.json"

        try:

            with open(filename, "w") as f:

                json.dump(
                    [x.to_dict() for x in pipeline._dataset],
                    f,
                    indent=2
                )

            print(f"Dataset saved as {filename}")

        except Exception as e:

            print(f"Could not save file: {e}")

        print("-" * 50)