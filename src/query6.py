# query6.py
from pathlib import Path
from DbConnector import DbConnector
import time
import csv

class FemaleProportionByDecadeQuery:
    def __init__(self):
        self.connection = DbConnector()
        self.db = self.connection.db

    def task_6_female_proportion_by_decade(self):
        """
        For each movie's top-billed 5 cast (by 'order'), compute proportion female
        (gender == 1 is female; gender == 2 male; ignore unknowns).
        Aggregate by decade and list decades sorted by average female proportion (desc),
        including movie counts used. Unknown gender ignored.
        """
        print("\nTask 6: Female proportion in top-5 cast, aggregated by decade")
        print("-" * 80)
        start = time.time()

        pipeline = [
            # parse release_date
            {
                "$addFields": {
                    "release_date_parsed": {
                        "$cond": [
                            {"$and": [{"$ne": ["$release_date", None]}, {"$ne": ["$release_date", ""]}]},
                            {"$dateFromString": {"dateString": "$release_date", "onError": None}},
                            None
                        ]
                    }
                }
            },

            # only movies with a release date and a cast array
            {
                "$match": {
                    "release_date_parsed": {"$ne": None},
                    "cast": {"$exists": True, "$ne": None}
                }
            },

            # compute decade label
            {
                "$addFields": {
                    "year": {"$year": "$release_date_parsed"},
                    "sorted_cast": {"$sortArray": {"input": "$cast", "sortBy": {"order": 1}}}
                }
            },

            # top 5
            {
                "$addFields": {
                    "top5": {"$slice": ["$sorted_cast", 5]}
                }
            },

            # compute female_count and known_count (exclude gender null/0)
            {
                "$addFields": {
                    "female_count": {
                        "$size": {
                            "$filter": {
                                "input": "$top5",
                                "as": "c",
                                "cond": {"$eq": ["$$c.gender", 1]}
                            }
                        }
                    },
                    "known_count": {
                        "$size": {
                            "$filter": {
                                "input": "$top5",
                                "as": "c",
                                "cond": {"$in": ["$$c.gender", [1, 2]]}
                            }
                        }
                    }
                }
            },

            # compute proportion (null when known_count == 0)
            {
                "$addFields": {
                    "female_proportion": {
                        "$cond": [
                            {"$eq": ["$known_count", 0]},
                            None,
                            {"$divide": ["$female_count", "$known_count"]}
                        ]
                    },
                    "decade_num": {"$multiply": [{"$floor": {"$divide": [{"$year": "$release_date_parsed"}, 10]}}, 10]},
                    "decade_label": {
                        "$concat": [
                            {"$toString": {"$multiply": [{"$floor": {"$divide": [{"$year": "$release_date_parsed"}, 10]}}, 10]}},
                            "s"
                        ]
                    }
                }
            },

            # Group by decade and compute average female_proportion and movie_count (only count movies with known_count>0)
            {
                "$group": {
                    "_id": {"decade_num": "$decade_num", "decade_label": "$decade_label"},
                    "avg_female_prop": {"$avg": "$female_proportion"},
                    "movie_count_all": {"$sum": 1},
                    # count only movies that contributed a proportion
                    "movie_count_with_gender": {"$sum": {"$cond": [{"$ne": ["$female_proportion", None]}, 1, 0]}}
                }
            },

            # projection
            {
                "$project": {
                    "_id": 0,
                    "decade_num": "$_id.decade_num",
                    "decade_label": "$_id.decade_label",
                    "avg_female_prop": 1,
                    "movie_count_all": 1,
                    "movie_count_with_gender": 1
                }
            },

            # sort by avg_female_prop desc
            {"$sort": {"avg_female_prop": -1}}
        ]

        results = list(self.db.movies.aggregate(pipeline))
        elapsed = time.time() - start

        print(f"\nQuery executed in {elapsed:.2f}s")
        print(f"Rows: {len(results)}\n")
        print("=" * 80)
        print(f"{'Decade':8} | {'AvgFemale%':9} | {'Movies(with gender)':18} | {'Movies(total)':12}")
        print("-" * 80)
        for r in results:
            avg = (r['avg_female_prop'] * 100) if isinstance(r.get('avg_female_prop'), (int, float)) else None
            avg_str = f"{avg:.1f}%" if avg is not None else "N/A"
            print(f"{r['decade_label']:8} | {avg_str:9} | {r['movie_count_with_gender']:18,} | {r['movie_count_all']:12,}")
        print("=" * 80)

        # Export CSV
        out = Path(__file__).resolve().parent.parent / "results" / "task6_female_prop_by_decade.csv"
        out.parent.mkdir(exist_ok=True)
        with open(out, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["decade_num", "decade_label", "avg_female_prop", "movie_count_with_gender", "movie_count_all"])
            for r in results:
                w.writerow([r.get("decade_num"), r.get("decade_label"), r.get("avg_female_prop"), r.get("movie_count_with_gender"), r.get("movie_count_all")])
        print(f"\nResults exported to: {out}")

        return results

    def close(self):
        self.connection.close_connection()

def main():
    executor = FemaleProportionByDecadeQuery()
    try:
        executor.task_6_female_proportion_by_decade()
    finally:
        executor.close()

if __name__ == "__main__":
    main()
