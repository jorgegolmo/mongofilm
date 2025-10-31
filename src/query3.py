from pathlib import Path
from DbConnector import DbConnector
import time

class MovieQueryExecutor:
    def __init__(self):
        self.connection = DbConnector()
        self.db = self.connection.db
        
    def query_top_actors_by_genre_breadth(self, min_movies=10, top_n=10, example_genres=5):

        
        start_time = time.time()
        
        pipeline = [
            {
                "$unwind": {
                    "path": "$cast",
                    "preserveNullAndEmptyArrays": False
                }
            },
            
            {
                "$unwind": {
                    "path": "$genres",
                    "preserveNullAndEmptyArrays": False
                }
            },
            
            {
                "$group": {
                    "_id": {
                        "actor_id": "$cast.id",
                        "actor_name": "$cast.name"
                    },
                    "distinct_genres": {"$addToSet": "$genres.name"},
                    "movie_count": {"$sum": 1}
                }
            },
            
            {
                "$project": {
                    "_id": 0,
                    "actor_id": "$_id.actor_id",
                    "actor_name": "$_id.actor_name",
                    "genre_count": {"$size": "$distinct_genres"},
                    "distinct_genres": 1,
                    "movie_count": 1
                }
            },
            
            {
                "$match": {
                    "movie_count": {"$gte": min_movies}
                }
            },
            
            {
                "$sort": {
                    "genre_count": -1,
                    "actor_name": 1
                }
            },
            
            {
                "$limit": top_n
            },
            
            {
                "$project": {
                    "actor_name": 1,
                    "actor_id": 1,
                    "genre_count": 1,
                    "movie_count": 1,
                    "example_genres": {"$slice": ["$distinct_genres", example_genres]},
                    "all_genres": "$distinct_genres"
                }
            }
        ]
        
        results = list(self.db.movies.aggregate(pipeline))
        
        elapsed = time.time() - start_time
        
        
        for i, actor in enumerate(results, 1):
            print(f"\n{i}. {actor['actor_name']}")
            print(f"   • Actor ID: {actor['actor_id']}")
            print(f"   • Géneros distintos: {actor['genre_count']}")
            print(f"   • Películas acreditadas: {actor['movie_count']}")
            print(f"   • Géneros de ejemplo: {', '.join(actor['example_genres'])}")
            
        
        return results
    
    def export_results_to_csv(self, results, output_path):
        import pandas as pd
        
        data = []
        for actor in results:
            data.append({
                'actor_id': actor['actor_id'],
                'actor_name': actor['actor_name'],
                'genre_count': actor['genre_count'],
                'movie_count': actor['movie_count'],
                'example_genres': ', '.join(actor['example_genres']),
                'all_genres': ', '.join(actor['all_genres'])
            })
        
        df = pd.DataFrame(data)
        df.to_csv(output_path, index=False)
    
    def close(self):
        self.connection.close_connection()

def main():
    executor = MovieQueryExecutor()
    
    try:
        results = executor.query_top_actors_by_genre_breadth(
            min_movies=10,
            top_n=10,
            example_genres=5
        )
        
        output_path = Path(__file__).resolve().parent.parent / "results" / "top_actors_genre_breadth.csv"
        output_path.parent.mkdir(exist_ok=True)
        executor.export_results_to_csv(results, output_path)
        
    finally:
        executor.close()

if __name__ == "__main__":
    main()
