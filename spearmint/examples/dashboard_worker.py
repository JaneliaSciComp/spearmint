#!/usr/bin/env python
"""Toy live-dashboard worker: 4 Hz metrics plus progressively arriving RGB PNG frames."""

import argparse
import json
import math
import random
import struct
import time
import zlib
from pathlib import Path

from spearmint import rundb


def _chunk(kind: bytes, data: bytes) -> bytes:
    return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data))


def write_png(path: Path, model: str, frame: int, size: int = 96) -> None:
    """Write a tiny dependency-free RGB heatmap whose pattern differs by model and frame."""
    shift = 0 if model == "a" else 17
    rows = bytearray()
    for y in range(size):
        rows.append(0)  # PNG filter: none
        for x in range(size):
            wave = (math.sin((x + frame * 4 + shift) / 10) + math.cos((y - frame * 3) / 13)) / 2
            hot = max(0, min(255, round((wave + 1) * 127.5)))
            rows.extend((hot, (x * 3 + frame * 13 + shift) % 256, 255 - hot))
    ihdr = struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(
        b"\x89PNG\r\n\x1a\n"
        + _chunk(b"IHDR", ihdr)
        + _chunk(b"IDAT", zlib.compress(bytes(rows)))
        + _chunk(b"IEND", b"")
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", choices=["a", "b"], required=True)
    args = parser.parse_args()

    with rundb.run() as run:
        rng = random.Random(args.model)
        metrics = Path(run.outdir) / "metrics.jsonl"
        history = []
        with metrics.open("w") as f:
            for tick in range(80):
                step = tick // 2
                view = ("xy", "xz")[tick % 2]
                offset = 0.08 if args.model == "b" else 0
                row = {
                    "step": step,
                    "view": view,
                    "loss": 1.5 * math.exp(-step / 18) + offset + rng.uniform(-0.025, 0.025),
                    "val_loss": 1.65 * math.exp(-step / 20) + offset + rng.uniform(-0.035, 0.035),
                    "accuracy": min(0.99, 0.45 + step / 85 - offset + rng.uniform(-0.01, 0.01)),
                    "iou": min(0.95, 0.30 + step / 100 - offset + rng.uniform(-0.015, 0.015)),
                }
                history.append(row)
                f.write(json.dumps(row) + "\n")
                f.flush()
                if tick % 8 == 0:
                    image_i = tick // 8
                    image_row = ("xy", "xz")[image_i % 2]
                    image_col = image_i // 2
                    for overlay, delta in (("raw", 0), ("prediction", 3)):
                        name = f"{image_row}_sharedbase_{image_col:02d}_{overlay}.png"
                        write_png(Path(run.outdir) / "frames" / name,
                                  args.model, image_i + delta)
                time.sleep(0.25)
        (Path(run.outdir) / "summary.json").write_text(json.dumps({
            "final_loss": history[-1]["loss"],
            "best_val_loss": min(row["val_loss"] for row in history),
            "final_accuracy": history[-1]["accuracy"],
            "best_iou": max(row["iou"] for row in history),
        }))


if __name__ == "__main__":
    main()
