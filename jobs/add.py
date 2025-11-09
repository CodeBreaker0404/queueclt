import sys

def run(payload):
    """Dynamic Python mode entry"""
    a = payload["a"]
    b = payload["b"]
    print("[ADD] Result =", a + b)
    return a + b

# CLI mode entry
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Error: expected 2 numbers", file=sys.stderr)
        sys.exit(1)

    a = int(sys.argv[1])
    b = int(sys.argv[2])
    print("[ADD] Result =", a + b)
