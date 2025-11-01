from pathlib import Path

def generate_json() -> str:

    file_path: Path = Path(__file__).parent.joinpath("dataclass_msg.txt")

    with file_path.open("w") as open_file:

        for i in range(1, 101):
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

    print(generate_json())