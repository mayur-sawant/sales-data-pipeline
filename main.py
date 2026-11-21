"""Project command-line entry point.

    python main.py generate   # create sample raw data
    python main.py pipeline   # run the ETL pipeline
    python main.py api        # start the FastAPI server
"""

from __future__ import annotations

import argparse
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description="Sales Data Processing Pipeline")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("generate", help="Generate sample raw sales data")
    sub.add_parser("pipeline", help="Run the ETL pipeline")
    api = sub.add_parser("api", help="Start the FastAPI server")
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=8000)
    api.add_argument("--reload", action="store_true")
    args, _ = parser.parse_known_args()

    # Strip sub-command so the delegated scripts see only their own arguments.
    sys.argv = [sys.argv[0]]
    if args.command == "generate":
        from scripts.generate_sample_data import main as generate_main

        return generate_main()
    if args.command == "pipeline":
        from scripts.run_pipeline import main as pipeline_main

        return pipeline_main()

    import uvicorn

    uvicorn.run("src.api.main:app", host=args.host, port=args.port, reload=args.reload)
    return 0


if __name__ == "__main__":
    sys.exit(main())
