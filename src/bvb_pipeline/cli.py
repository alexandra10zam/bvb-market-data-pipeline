import argparse
import json
import logging
from datetime import date, timedelta

from . import load
from .config import DEFAULT_TICKERS, Settings
from .pipeline import build_indicators, extract_and_validate, run


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="bvb-pipeline", description="Load BVB prices into PostgreSQL")
    parser.add_argument("--start", type=date.fromisoformat, default=date.today() - timedelta(days=7))
    parser.add_argument("--end", type=date.fromisoformat, default=date.today())
    parser.add_argument("--tickers", nargs="+", default=DEFAULT_TICKERS)
    parser.add_argument("--source", choices=["yahoo", "synthetic"], default="yahoo")
    parser.add_argument("--step", choices=["all", "extract", "indicators"], default="all",
                        help="run one stage only (used by the Airflow DAG)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = Settings()
    if args.step == "all":
        result = run(settings, args.tickers, args.start, args.end, args.source)
    else:
        eng = load.engine(settings.database_url)
        load.ensure_schema(eng)
        if args.step == "extract":
            result = extract_and_validate(eng, settings, args.tickers, args.start, args.end, args.source)
        else:
            result = {"indicator_rows": build_indicators(eng, settings, args.tickers, args.start)}
    print(json.dumps(result, default=str, indent=2))


if __name__ == "__main__":
    main()
