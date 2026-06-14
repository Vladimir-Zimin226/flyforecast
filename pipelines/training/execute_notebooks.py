import argparse
from pathlib import Path

import nbformat
from nbclient import NotebookClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Execute selected project notebooks in place.")
    parser.add_argument("notebooks", nargs="+", help="Notebook paths relative to the project root.")
    parser.add_argument("--timeout", type=int, default=900)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path.cwd().resolve()

    for notebook_arg in args.notebooks:
        notebook_path = (project_root / notebook_arg).resolve()
        print(f"Executing {notebook_path}")
        notebook = nbformat.read(notebook_path, as_version=4)
        client = NotebookClient(
            notebook,
            timeout=args.timeout,
            kernel_name="python3",
            resources={"metadata": {"path": str(project_root)}},
        )
        client.execute()
        nbformat.write(notebook, notebook_path)
        print(f"Saved {notebook_path}")


if __name__ == "__main__":
    main()
