from pathlib import Path
from DbConnector import DbConnector
import time
import csv

class CollectionRevenueQuery:
    def __init__(self):
        print("🔌 Conectando a MongoDB...")
        self.connection = DbConnector()
        self.db = self.connection.db

    def task_4_top_collections(self, top_n=10):
        """
        Task 4:
        For film collections (belongs_to_collection.name not null) with >= 3 movies,
        find the top `top_n` collections by total revenue.
        Report: movie count, total revenue, median vote_average, earliest -> latest release date.
        """
        print("\n🎬 Task 4: Top {} collections by total revenue".format(top_n))
        print("-" * 80)

        start = time.time()

        pipeline = [
            # Only movies that belong to a collection with a (non-empty) name
            {
                "$match": {
                    "belongs_to_collection": {"$exists": True, "$ne": None},
                    "belongs_to_collection.name": {"$exists": True, "$ne": ""}
                }
            },

            # Normalize fields: revenue -> 0 if missing, keep vote_average (may be null),
            # parse release_date to a date (null if invalid/empty)
            {
                "$addFields": {
                    "revenue": {"$ifNull": ["$revenue", 0]},
                    "vote_average": {"$ifNull": ["$vote_average", None]},
                    "release_date_parsed": {
                        "$cond": [
                            {"$and": [{"$ne": ["$release_date", None]}, {"$ne": ["$release_date", ""]}]},
                            {"$dateFromString": {"dateString": "$release_date", "onError": None}},
                            None
                        ]
                    }
                }
            },

            # Group by collection id + name
            {
                "$group": {
                    "_id": {
                        "collection_id": "$belongs_to_collection.id",
                        "collection_name": "$belongs_to_collection.name"
                    },
                    "movie_count": {"$sum": 1},
                    "total_revenue": {"$sum": "$revenue"},
                    # collect vote_averages into an array (some entries may be null)
                    "votes": {"$push": "$vote_average"},
                    "earliest_release": {"$min": "$release_date_parsed"},
                    "latest_release": {"$max": "$release_date_parsed"}
                }
            },

            # Keep only collections with at least 3 movies
            {"$match": {"movie_count": {"$gte": 3}}},

            # Prepare votes: remove nulls then sort them (must use sortBy with $sortArray)
            {
                "$project": {
                    "collection_id": "$_id.collection_id",
                    "collection_name": "$_id.collection_name",
                    "movie_count": 1,
                    "total_revenue": 1,
                    "votes_filtered": {
                        "$filter": {
                            "input": "$votes",
                            "as": "v",
                            "cond": {"$ne": ["$$v", None]}
                        }
                    },
                    "earliest_release": 1,
                    "latest_release": 1
                }
            },

            # Sort the filtered votes ascending
            {
                "$addFields": {
                    "sorted_votes": {
                        "$sortArray": {
                            "input": "$votes_filtered",
                            "sortBy": 1
                        }
                    }
                }
            },

            # n = size, mid = floor(n/2)
            {
                "$addFields": {
                    "n": {"$size": "$sorted_votes"},
                    "mid": {"$floor": {"$divide": [{"$size": "$sorted_votes"}, 2]}}
                }
            },

            # median calculation:
            # - if n == 0 -> null
            # - if odd -> element at mid
            # - if even -> average of elements at mid-1 and mid
            {
                "$addFields": {
                    "median_vote_average": {
                        "$cond": [
                            {"$eq": ["$n", 0]},
                            None,
                            {
                                "$cond": [
                                    {"$eq": [{"$mod": ["$n", 2]}, 1]},  # odd
                                    {"$arrayElemAt": ["$sorted_votes", "$mid"]},
                                    # even
                                    {
                                        "$cond": [
                                            {"$gte": ["$n", 2]},
                                            {
                                                "$avg": [
                                                    {"$arrayElemAt": ["$sorted_votes", {"$subtract": ["$mid", 1]}]},
                                                    {"$arrayElemAt": ["$sorted_votes", "$mid"]}
                                                ]
                                            },
                                            None
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                }
            },

            # Format release dates back to strings (YYYY-MM-DD); keep fields we need
            {
                "$project": {
                    "_id": 0,
                    "collection_id": 1,
                    "collection_name": 1,
                    "movie_count": 1,
                    "total_revenue": 1,
                    "median_vote_average": 1,
                    "earliest_release": {
                        "$cond": [
                            {"$ne": ["$earliest_release", None]},
                            {"$dateToString": {"format": "%Y-%m-%d", "date": "$earliest_release"}},
                            None
                        ]
                    },
                    "latest_release": {
                        "$cond": [
                            {"$ne": ["$latest_release", None]},
                            {"$dateToString": {"format": "%Y-%m-%d", "date": "$latest_release"}},
                            None
                        ]
                    }
                }
            },

            # Sort by total_revenue desc
            {"$sort": {"total_revenue": -1}},

            # Limit to top_n
            {"$limit": top_n}
        ]

        try:
            results = list(self.db.movies.aggregate(pipeline))
        except Exception as e:
            print("\n❌ ERROR running aggregation:", e)
            raise

        elapsed = time.time() - start

        # Print results nicely
        print(f"\n✅ Query executed in {elapsed:.2f}s")
        print(f"📋 Top {len(results)} collections by total revenue\n")
        print("=" * 80)
        for i, r in enumerate(results, start=1):
            total_rev = r.get("total_revenue") or 0
            median_vote = r.get("median_vote_average")
            med_str = f"{median_vote:.2f}" if isinstance(median_vote, (int, float)) else "N/A"
            earliest = r.get("earliest_release") or "N/A"
            latest = r.get("latest_release") or "N/A"
            print(f"{i}. {r.get('collection_name')}")
            print(f"   • Collection ID: {r.get('collection_id')}")
            print(f"   • Movies in collection: {r.get('movie_count'):,}")
            print(f"   • Total revenue: ${total_rev:,}")
            print(f"   • Median vote_average: {med_str}")
            print(f"   • Release range: {earliest} → {latest}")
            print("-" * 80)

        # Export to CSV (optional; helpful for inspections)
        out_path = Path(__file__).resolve().parent.parent / "results" / "task4_collections_by_revenue.csv"
        out_path.parent.mkdir(exist_ok=True)
        with open(out_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["rank", "collection_id", "collection_name", "movie_count", "total_revenue", "median_vote_average", "earliest_release", "latest_release"])
            for i, r in enumerate(results, start=1):
                writer.writerow([
                    i,
                    r.get("collection_id"),
                    r.get("collection_name"),
                    r.get("movie_count"),
                    r.get("total_revenue"),
                    (round(r.get("median_vote_average"), 2) if isinstance(r.get("median_vote_average"), (int, float)) else ""),
                    r.get("earliest_release") or "",
                    r.get("latest_release") or ""
                ])
        print(f"\n💾 Results exported to: {out_path}")

        return results

    def close(self):
        self.connection.close_connection()


def main():
    executor = CollectionRevenueQuery()
    try:
        executor.task_4_top_collections(top_n=10)
    finally:
        executor.close()


if __name__ == "__main__":
    main()
