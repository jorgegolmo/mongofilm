# query9.py
from pathlib import Path
from DbConnector import DbConnector
import time
import csv

class NonEnglishUSProductionQuery:
    def __init__(self):
        self.connection = DbConnector()
        self.db = self.connection.db

    def task_9_top_original_languages(self, top_n=10):
        """
        Among movies where original_language != "en" but at least one production company
        or production country is United States, find top original languages by count.
        For each language, return count and one example title.
        """
        print("\nTask 9: Top original languages (non-en) with US involvement")
        print("-" * 80)
        start = time.time()

        pipeline = [
            # Match non-English originals
            {"$match": {"original_language": {"$ne": "en"}}},

            # At least one production_country of US (check by iso_3166_1) OR production_company origin_country == 'US'
            {"$match": {
                "$or": [
                    {"production_countries": {"$elemMatch": {"iso_3166_1": "US"}}},
                    {"production_countries": {"$elemMatch": {"name": "United States of America"}}},
                    {"production_companies": {"$elemMatch": {"origin_country": "US"}}}
                ]
            }},

            # Group by original_language, count and grab an example title
            {
                "$group": {
                    "_id": "$original_language",
                    "count": {"$sum": 1},
                    "example_title": {"$first": "$title"}
                }
            },

            {"$sort": {"count": -1}},
            {"$limit": top_n},
            {
                "$project": {
                    "_id": 0,
                    "original_language": "$_id",
                    "count": 1,
                    "example_title": 1
                }
            }
        ]

        results = list(self.db.movies.aggregate(pipeline))
        elapsed = time.time() - start

        print(f"\nQuery executed in {elapsed:.2f}s")
        print(f"Top {len(results)} original languages:\n")
        print("=" * 80)
        print(f"{'Lang':6} | {'Count':6} | {'Example title'}")
        print("-" * 80)
        for r in results:
            print(f"{r['original_language']:6} | {r['count']:6,} | {r['example_title']}")
        print("=" * 80)

        # Export CSV
        out = Path(__file__).resolve().parent.parent / "results" / "task9_original_languages_us.csv"
        out.parent.mkdir(exist_ok=True)
        with open(out, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["original_language", "count", "example_title"])
            for r in results:
                w.writerow([r.get("original_language"), r.get("count"), r.get("example_title")])
        print(f"\nResults exported to: {out}")

        return results

    def close(self):
        self.connection.close_connection()

def main():
    executor = NonEnglishUSProductionQuery()
    try:
        executor.task_9_top_original_languages(top_n=10)
    finally:
        executor.close()

if __name__ == "__main__":
    main()
