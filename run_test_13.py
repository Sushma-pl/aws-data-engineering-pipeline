"""Thin wrapper to run test_pipeline.py and capture output."""
import subprocess, sys

result = subprocess.run(
    [sys.executable, "-m", "pytest", "tests/unit/test_pipeline.py", "-v", "--tb=short"],
    capture_output=True,
    text=True,
)
print(result.stdout)
print(result.stderr)
print("Return code:", result.returncode)
