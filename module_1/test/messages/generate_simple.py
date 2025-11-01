from pathlib import Path

def generate_simple_messages() -> str:

    file_path: Path = Path(__file__).parent.joinpath("simple_msg.txt")

    with file_path.open("w") as open_file:

        for i in range(1, 101):
            open_file.write(f"Simple message #{i}\n")

    return str(file_path)

if __name__ == "__main__":

    print(generate_simple_messages())
    