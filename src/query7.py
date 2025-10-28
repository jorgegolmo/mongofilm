# query7.py
from pathlib import Path
from DbConnector import DbConnector
import time
import csv
import re

class NoirSearchQuery:
    def __init__(self):
        print("🔌 Conectando a MongoDB...")
        self.connection = DbConnector()
        self.db = self.connection.db

    def task_7_top_noir_movies(self, top_n=20):
        """
        Text (or regex) search over overview and tagline for 'noir' or 'neo-noir'
        (case-insensitive). Filter vote_count >= 50. Return top `top_n` by vote_average.
        """
        print("\n🎬 Task 7: Top movies matching 'noir' / 'neo-noir' (vote_count >= 50)")
        print("-" * 80)
        start = time.time()

        # regex to match 'noir' or 'neo-noir' (word boundaries), case-insensitive
        pattern = r"\b(?:neo-)?noir\b"
        regex = {"$regex": pattern, "$options": "i"}

        pipeline = [
            {
                "$match": {
                    "vote_count": {"$gte": 50},
                    "$or": [
                        {"overview": regex},
                        {"tagline": regex}
                    ]
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "title": 1,
                    "release_date": 1,
                    "vote_average": 1,
                    "vote_count": 1,
                    "year": {
                        "$cond": [
                            {"$and": [{"$ne": ["$release_date", None]}, {"$ne": ["$release_date", ""]}]},
                            {"$year": {"$dateFromString": {"dateString": "$release_date", "onError": None}}},
                            None
                        ]
                    }
                }
            },
            {"$sort": {"vote_average": -1, "vote_count": -1}},
            {"$limit": top_n}
        ]

        results = list(self.db.movies.aggregate(pipeline))
        elapsed = time.time() - start

        print(f"\n✅ Query executed in {elapsed:.2f}s")
        print(f"📋 Top {len(results)} matching movies:\n")
        print("=" * 80)
        print(f"{'Title':50} | {'Year':4} | {'vote_avg':8} | {'vote_count':10}")
        print("-" * 80)
        for r in results:
            title = (r.get("title") or "")[:50]
            year = r.get("year") or (r.get("release_date") or "")[:10]
            print(f"{title:50} | {str(year):4} | {r.get('vote_average'):8} | {r.get('vote_count'):10,}")
        print("=" * 80)

        # Export CSV
        out = Path(__file__).resolve().parent.parent / "results" / "task7_noir_top20.csv"
        out.parent.mkdir(exist_ok=True)
        with open(out, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=["title", "year", "release_date", "vote_average", "vote_count"])
            w.writeheader()
            for r in results:
                w.writerow({
                    "title": r.get("title"),
                    "year": r.get("year"),
                    "release_date": r.get("release_date"),
                    "vote_average": r.get("vote_average"),
                    "vote_count": r.get("vote_count")
                })
        print(f"\n💾 Results exported to: {out}")

        return results

    def close(self):
        self.connection.close_connection()

def main():
    executor = NoirSearchQuery()
    try:
        executor.task_7_top_noir_movies(top_n=20)
    finally:
        executor.close()

if __name__ == "__main__":
    main()
