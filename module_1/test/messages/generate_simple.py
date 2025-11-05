import sys
from pathlib import Path

def generate_simple_messages(message_count: str) -> str:

    file_path: Path = Path(__file__).parent.joinpath("simple_msg.txt")

    with file_path.open("w") as open_file:

        for i in range(1, int(message_count) + 1):
            open_file.write(f"Simple message #{i}\n")

    return str(file_path)

if __name__ == "__main__":
    message_count: str = sys.argv[1] if len(sys.argv) > 1 else "30"
    print(generate_simple_messages(message_count))
    