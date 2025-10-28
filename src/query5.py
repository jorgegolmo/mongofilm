# query5.py
from pathlib import Path
from DbConnector import DbConnector
import time
import csv

class DecadeGenreRuntimeQuery:
    def __init__(self):
        print("🔌 Conectando a MongoDB...")
        self.connection = DbConnector()
        self.db = self.connection.db

    def task_5_median_runtime_by_decade_genre(self):
        """
        By decade and primary genre (first element in genres),
        compute median runtime and movie count.
        Sort by decade ascending then median runtime descending.
        """
        print("\n🎬 Task 5: Median runtime and movie count by decade & primary genre")
        print("-" * 80)
        start = time.time()

        pipeline = [
            # Keep movies with a release_date and runtime
            {
                "$addFields": {
                    "release_date_parsed": {
                        "$cond": [
                            {"$and": [{"$ne": ["$release_date", None]}, {"$ne": ["$release_date", ""]}]},
                            {"$dateFromString": {"dateString": "$release_date", "onError": None}},
                            None
                        ]
                    },
                    "runtime": {"$ifNull": ["$runtime", None]},
                    # primary genre name: first element's name (if exists)
                    "primary_genre": {
                        "$let": {
                            "vars": {
                                "g": {"$ifNull": ["$genres", []]}
                            },
                            "in": {
                                "$cond": [
                                    {"$gt": [{"$size": "$$g"}, 0]},
                                    {"$arrayElemAt": ["$$g.name", 0]},
                                    None
                                ]
                            }
                        }
                    }
                }
            },

            # Filter out docs without parsed release date or runtime or primary_genre
            {
                "$match": {
                    "release_date_parsed": {"$ne": None},
                    "runtime": {"$ne": None},
                    "primary_genre": {"$ne": None}
                }
            },

            # Compute decade number and label
            {
                "$addFields": {
                    "year": {"$year": "$release_date_parsed"}
                }
            },
            {
                "$addFields": {
                    "decade_num": {"$multiply": [{"$floor": {"$divide": ["$year", 10]}}, 10]},
                    "decade_label": {
                        "$concat": [
                            {"$toString": {"$multiply": [{"$floor": {"$divide": ["$year", 10]}}, 10]}},
                            "s"
                        ]
                    }
                }
            },

            # Group by decade and primary_genre
            {
                "$group": {
                    "_id": {"decade_num": "$decade_num", "decade_label": "$decade_label", "primary_genre": "$primary_genre"},
                    "movie_count": {"$sum": 1},
                    "runtimes": {"$push": "$runtime"}
                }
            },

            # Prepare sorted runtimes
            {
                "$addFields": {
                    "sorted_runtimes": {
                        "$sortArray": {"input": "$runtimes", "sortBy": 1}
                    },
                    "n": {"$size": "$runtimes"}
                }
            },

            # median calculation
            {
                "$addFields": {
                    "mid": {"$floor": {"$divide": ["$n", 2]}},
                    "median_runtime": {
                        "$cond": [
                            {"$eq": ["$n", 0]},
                            None,
                            {
                                "$cond": [
                                    {"$eq": [{"$mod": ["$n", 2]}, 1]},
                                    {"$arrayElemAt": ["$sorted_runtimes", {"$floor": {"$divide": ["$n", 2]}}]},
                                    {
                                        "$avg": [
                                            {"$arrayElemAt": ["$sorted_runtimes", {"$subtract": [{"$floor": {"$divide": ["$n", 2]}}, 1]}]},
                                            {"$arrayElemAt": ["$sorted_runtimes", {"$floor": {"$divide": ["$n", 2]}}]}
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                }
            },

            # Projection
            {
                "$project": {
                    "_id": 0,
                    "decade_num": "$_id.decade_num",
                    "decade_label": "$_id.decade_label",
                    "primary_genre": "$_id.primary_genre",
                    "movie_count": 1,
                    "median_runtime": 1
                }
            },

            # Sort by decade ascending, median runtime desc
            {"$sort": {"decade_num": 1, "median_runtime": -1}}
        ]

        results = list(self.db.movies.aggregate(pipeline))
        elapsed = time.time() - start

        # Print results
        print(f"\n✅ Query executed in {elapsed:.2f}s")
        print(f"📊 Rows: {len(results)}\n")
        print("=" * 80)
        header = f"{'Decade':8} | {'Genre':30} | {'Movies':6} | {'Median runtime':13}"
        print(header)
        print("-" * 80)
        for r in results:
            med = ("{:.1f}".format(r['median_runtime']) if isinstance(r.get('median_runtime'), (int, float)) else "N/A")
            print(f"{r['decade_label']:8} | {r['primary_genre'][:30]:30} | {r['movie_count']:6,} | {med:13}")
        print("=" * 80)

        # Export CSV
        out = Path(__file__).resolve().parent.parent / "results" / "task5_decade_genre_runtime.csv"
        out.parent.mkdir(exist_ok=True)
        with open(out, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["decade_num", "decade_label", "primary_genre", "movie_count", "median_runtime"])
            for r in results:
                w.writerow([r.get("decade_num"), r.get("decade_label"), r.get("primary_genre"), r.get("movie_count"), r.get("median_runtime")])
        print(f"\n💾 Results exported to: {out}")

        return results

    def close(self):
        self.connection.close_connection()

def main():
    executor = DecadeGenreRuntimeQuery()
    try:
        executor.task_5_median_runtime_by_decade_genre()
    finally:
        executor.close()

if __name__ == "__main__":
    main()
