import json

from pyspark.sql.streaming import StreamingQueryListener


class MetricsListener(StreamingQueryListener):
    """Emits one JSON log line per micro-batch: throughput and lag indicators."""

    def onQueryStarted(self, event):
        print(json.dumps({"event": "query_started", "name": event.name}), flush=True)

    def onQueryProgress(self, event):
        p = event.progress
        print(json.dumps({
            "event": "progress", "query": p.name, "batch_id": p.batchId,
            "input_rows": p.numInputRows,
            "input_rows_per_sec": p.inputRowsPerSecond,
            "processed_rows_per_sec": p.processedRowsPerSecond,
        }), flush=True)

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        print(json.dumps({"event": "query_terminated", "exception": event.exception}), flush=True)
