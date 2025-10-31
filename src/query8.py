# query8.py
from pathlib import Path
from DbConnector import DbConnector
import time
import csv

class DirectorActorPairsQuery:
    def __init__(self):
        self.connection = DbConnector()
        self.db = self.connection.db

    def task_8_top_director_actor_pairs(self, min_collabs=3, top_n=20):
        """
        Among movies with vote_count >= 100, find director-actor pairs that collaborated >= min_collabs times.
        Return top_n pairs by mean vote_average. Also include films_count and mean_revenue.
        """
        print(f"\nTask 8: Director–actor pairs with ≥ {min_collabs} collaborations (vote_count ≥ 100)")
        print("-" * 80)
        start = time.time()

        pipeline = [
            # Consider only movies with sufficient votes
            {"$match": {"vote_count": {"$gte": 100}}},
            # unwind crew and filter for Directors
            {"$unwind": {"path": "$crew"}},
            {"$match": {"crew.job": "Director"}},
            # unwind cast
            {"$unwind": {"path": "$cast"}},
            # group by director + actor pair
            {
                "$group": {
                    "_id": {
                        "director": "$crew.name",
                        "actor": "$cast.name"
                    },
                    "films_count": {"$sum": 1},
                    "mean_vote": {"$avg": "$vote_average"},
                    "mean_revenue": {"$avg": {"$ifNull": ["$revenue", 0]}},
                    "titles": {"$push": "$title"}
                }
            },
            # keep pairs with enough collaborations
            {"$match": {"films_count": {"$gte": min_collabs}}},
            # sort by mean_vote desc
            {"$sort": {"mean_vote": -1}},
            {"$limit": top_n},
            # project result
            {
                "$project": {
                    "_id": 0,
                    "director": "$_id.director",
                    "actor": "$_id.actor",
                    "films_count": 1,
                    "mean_vote": 1,
                    "mean_revenue": 1,
                    "titles": 1
                }
            }
        ]

        results = list(self.db.movies.aggregate(pipeline))
        elapsed = time.time() - start

        print(f"\nQuery executed in {elapsed:.2f}s")
        print(f"Top {len(results)} director–actor pairs:\n")
        print("=" * 80)
        for i, r in enumerate(results, start=1):
            print(f"{i}. {r['director']} — {r['actor']}")
            print(f"   • Films together: {r['films_count']}")
            print(f"   • Mean vote_average: {r['mean_vote']:.3f}")
            print(f"   • Mean revenue: ${int(r['mean_revenue']):,}")
            print(f"   • Example titles: {', '.join((r.get('titles') or [])[:5])}")
            print("-" * 80)

        # Export CSV
        out = Path(__file__).resolve().parent.parent / "results" / "task8_director_actor_pairs.csv"
        out.parent.mkdir(exist_ok=True)
        with open(out, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["rank", "director", "actor", "films_count", "mean_vote", "mean_revenue", "example_titles"])
            for i, r in enumerate(results, start=1):
                w.writerow([i, r.get("director"), r.get("actor"), r.get("films_count"), r.get("mean_vote"), r.get("mean_revenue"), "; ".join((r.get("titles") or [])[:5])])
        print(f"\nResults exported to: {out}")

        return results

    def close(self):
        self.connection.close_connection()

def main():
    executor = DirectorActorPairsQuery()
    try:
        executor.task_8_top_director_actor_pairs(min_collabs=3, top_n=20)
    finally:
        executor.close()

if __name__ == "__main__":
    main()
