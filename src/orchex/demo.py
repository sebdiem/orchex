from __future__ import annotations

import random
import time
from typing import Any

from .dag import Dag

dag = Dag("demo")


@dag.task(name="extract_text")
def extract_text(inputs: dict[str, Any]) -> dict[str, Any]:
    time.sleep(0.2)
    uri = inputs.get("uri", "memory://unknown")
    return {"text": f"Hello from {uri}"}


@dag.task(name="ner", requires=["extract_text"])
def ner(inputs: dict[str, Any]) -> dict[str, Any]:
    text = inputs["extract_text"]["text"]
    time.sleep(0.2)
    if random.random() < 0.5:
        raise RuntimeError("no luck")
    return {"entities": [{"type": "GREETING", "value": text.split()[0]}]}


@dag.task(name="classify", requires=["extract_text"], timeout=1)
def classify(inputs: dict[str, Any]) -> dict[str, Any]:
    text = inputs["extract_text"]["text"]
    sleep_for = 0.2 if random.random() < 0.5 else 1.2
    time.sleep(sleep_for)  # should trigger a timeout half of the time
    if sleep_for > 1:
        print("################# task was no killed #####################")
    return {"topic": "demo" if "Hello" in text else "other"}


@dag.task(name="enrich", requires=["ner", "classify"])
def enrich(inputs: dict[str, Any]) -> dict[str, Any]:
    time.sleep(0.1)
    return {
        "summary": {
            "entity_count": len(inputs["ner"]["entities"]),
            "topic": inputs["classify"]["topic"],
        }
    }


__all__ = ["dag"]
