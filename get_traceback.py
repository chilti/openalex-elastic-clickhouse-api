import traceback
import sys

try:
    import app
    print("App imported successfully")
except Exception:
    with open("traceback_captured.txt", "w") as f:
        traceback.print_exc(file=f)
    print("Traceback captured to traceback_captured.txt")
    sys.exit(1)
