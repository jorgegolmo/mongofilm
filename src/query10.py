# query10.py
from pathlib import Path
from DbConnector import DbConnector
import time
import pandas as pd

class UserRatingsStatsExecutor:
    def __init__(self):
        self.connection = DbConnector()
        self.db = self.connection.db

    def ensure_indexes(self):
        """
        Create helpful indexes (run once).
        """
        print("\nEnsuring indexes (movies.tmdbId, ratings.tmdbId, ratings.userId)...")
        try:
            self.db.movies.create_index("tmdbId", name="idx_movies_tmdbId")
            self.db.ratings.create_index("tmdbId", name="idx_ratings_tmdbId")
            self.db.ratings.create_index("userId", name="idx_ratings_userId")
            print("   ✓ Indexes created/ensured.")
        except Exception as e:
            print("Error creating indexes:", e)

    def task_10_user_stats_optimized(self, top_n=10, min_ratings_for_variance=20, example_genres=5):
        """
        Optimized Task 10:
         - Aggregate ratings per user (count, sum, sumsq, distinct movie ids)
         - Lookup movies once per user to extract unique genres
         - Compute population variance and distinct genre count
         - Return two leaderboards:
             * top_genre_diverse (by distinct genre count)
             * top_variance (by population variance, with min ratings threshold)
        """
        print("\nTask 10 (optimized): User rating stats (count, population variance, distinct genres)")
        print("-" * 90)

        start_time = time.time()

        pipeline = [
            # 1) only ratings with a movie link (tmdbId) — skip if you have only movieId, change needed
            {"$match": {"tmdbId": {"$exists": True, "$ne": None}}},
            # 2) compress ratings per user: counts, sum, sumsq, and distinct movie ids
            {
                "$group": {
                    "_id": "$userId",
                    "rating_count": {"$sum": 1},
                    "rating_sum": {"$sum": "$rating"},
                    "rating_sumsq": {"$sum": {"$multiply": ["$rating", "$rating"]}},
                    "movie_ids": {"$addToSet": "$tmdbId"}
                }
            },
            # 3) lookup all movies for that user's distinct movie list (single lookup per user)
            {
                "$lookup": {
                    "from": "movies",
                    "localField": "movie_ids",
                    "foreignField": "tmdbId",
                    "as": "movies"
                }
            },
            # 4) build a single set of unique genre names across the user's movies
            {
                "$project": {
                    "_id": 0,
                    "userId": "$_id",
                    "rating_count": 1,
                    "rating_sum": 1,
                    "rating_sumsq": 1,
                    "movie_count_distinct": {"$size": "$movie_ids"},
                    # genres_all: set-union across movies -> each movie -> map genres to names -> union
                    "genres_all": {
                        "$reduce": {
                            "input": {
                                "$map": {
                                    "input": {"$ifNull": ["$movies", []]},
                                    "as": "m",
                                    "in": {
                                        # map genres array to names; if missing genres -> empty array
                                        "$ifNull": [
                                            {
                                                "$map": {
                                                    "input": {"$ifNull": ["$$m.genres", []]},
                                                    "as": "g",
                                                    "in": "$$g.name"
                                                }
                                            },
                                            []
                                        ]
                                    }
                                }
                            },
                            "initialValue": [],
                            "in": {"$setUnion": ["$$value", "$$this"]}
                        }
                    },
                    # example slice
                    "example_genres": {"$slice": ["$genres_all", example_genres]},
                    # population variance formula:
                    # var = (sumsq - (sum^2)/n) / n
                    "population_variance": {
                        "$cond": [
                            {"$gt": ["$rating_count", 0]},
                            {
                                "$divide": [
                                    {
                                        "$subtract": [
                                            "$rating_sumsq",
                                            {"$divide": [{"$multiply": ["$rating_sum", "$rating_sum"]}, "$rating_count"]}
                                        ]
                                    },
                                    "$rating_count"
                                ]
                            },
                            None
                        ]
                    }
                }
            },
            # 5) facet for both leaderboards
            {
                "$facet": {
                    "top_genre_diverse": [
                        {"$sort": {"genres_all": -1}},  # sort by array length won't work — so sort by computed field below
                        # Instead sort by size of genres_all using $addFields then $sort:
                        {"$addFields": {"distinct_genre_count": {"$size": "$genres_all"}}},
                        {"$sort": {"distinct_genre_count": -1, "rating_count": -1, "userId": 1}},
                        {"$limit": top_n}
                    ],
                    "top_variance": [
                        {"$match": {"rating_count": {"$gte": min_ratings_for_variance}}},
                        {"$sort": {"population_variance": -1, "rating_count": -1, "userId": 1}},
                        {"$limit": top_n}
                    ]
                }
            }
        ]

        # run aggregation (allowDiskUse helps with memory)
        try:
            cursor = self.db.ratings.aggregate(pipeline, allowDiskUse=True)
            docs = list(cursor)
            if not docs:
                print("Aggregation returned no documents.")
                return {"top_genre_diverse": [], "top_variance": []}
            agg_result = docs[0]
        except Exception as e:
            print("\nERROR running aggregation:", e)
            raise

        elapsed = time.time() - start_time
        print(f"\nAggregation completed in {elapsed:.2f}s (server-side).")
        print(f"   • Retrieved {len(agg_result.get('top_genre_diverse', []))} genre-diverse rows and {len(agg_result.get('top_variance', []))} variance rows.")

        # pretty print
        print("\n" + "="*90)
        print("TOP USERS BY DISTINCT GENRES RATED")
        print("="*90)
        for i, u in enumerate(agg_result.get("top_genre_diverse", []), 1):
            genres = u.get("example_genres") or []
            print(f"\n{i}. userId: {u['userId']}")
            print(f"   • Distinct genres: {u.get('distinct_genre_count', len(u.get('genres_all', [])))}")
            print(f"   • Distinct movies rated: {u.get('movie_count_distinct')}")
            print(f"   • Ratings count: {u.get('rating_count')}")
            print(f"   • Example genres: {', '.join(genres)}")

        print("\n" + "="*90)
        print(f"TOP USERS BY POPULATION VARIANCE (min {min_ratings_for_variance} ratings)")
        print("="*90)
        for i, u in enumerate(agg_result.get("top_variance", []), 1):
            var_val = u.get("population_variance")
            var_str = f"{var_val:.4f}" if (var_val is not None) else "N/A"
            ex = u.get("example_genres") or []
            print(f"\n{i}. userId: {u['userId']}")
            print(f"   • Population variance: {var_str}")
            print(f"   • Ratings count: {u.get('rating_count')}")
            print(f"   • Distinct genres: {len(u.get('genres_all', []))}")
            print(f"   • Example genres: {', '.join(ex)}")

        # export CSVs
        out_dir = Path(__file__).resolve().parent.parent / "results"
        out_dir.mkdir(parents=True, exist_ok=True)

        df_genre = pd.DataFrame(agg_result.get("top_genre_diverse", []))
        df_var = pd.DataFrame(agg_result.get("top_variance", []))

        if not df_genre.empty:
            df_genre.to_csv(out_dir / "task10_top_genre_diverse_users_optimized.csv", index=False)
            print(f"\nExported genre-diverse leaderboard to: {out_dir / 'task10_top_genre_diverse_users_optimized.csv'}")
        if not df_var.empty:
            df_var.to_csv(out_dir / "task10_top_variance_users_optimized.csv", index=False)
            print(f"Exported variance leaderboard to: {out_dir / 'task10_top_variance_users_optimized.csv'}")

        return agg_result

    def close(self):
        self.connection.close_connection()

def main():
    executor = UserRatingsStatsExecutor()
    try:
        # optional: create indexes once (uncomment if you haven't created them)
        executor.ensure_indexes()

        executor.task_10_user_stats_optimized(top_n=10, min_ratings_for_variance=20, example_genres=5)
    finally:
        executor.close()

if __name__ == "__main__":
    main()
