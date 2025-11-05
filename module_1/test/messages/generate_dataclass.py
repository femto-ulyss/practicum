import sys
from pathlib import Path

def generate_json(message_count: str) -> str:

    file_path: Path = Path(__file__).parent.joinpath("dataclass_msg.txt")

    with file_path.open("w") as open_file:

        for i in range(1, int(message_count) + 1):
            json_str: str = (
                "{"
                    f'"sku_id": {i}, '
                    '"unit": "box", '
                    '"store": [1, 2, 3], '
                    '"is_limited": "true", '
                    '"metadata": {"source": "market", "start_dttm": "2000-01-01 00:00:00"}, '
                    '"tags": null'
                "}\n"
            )

            open_file.write(json_str)

    return str(file_path)


if __name__ == "__main__":
    message_count: str = sys.argv[1] if len(sys.argv) > 1 else "30"
    print(generate_json(message_count))