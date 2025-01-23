from pathlib import Path

exception = Path("exception")
exception.mkdir(parents=True, exist_ok=True)

with open(exception/"test.txt", "w") as f:
    f.write("test")