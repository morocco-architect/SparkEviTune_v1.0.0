from __future__ import annotations

import json

from sparkevitune.pipeline import SparkEviTunePipeline
from sparkevitune.utils import to_jsonable

if __name__ == "__main__":
    summary = SparkEviTunePipeline().train_models()
    print(json.dumps(to_jsonable(summary), indent=2))
