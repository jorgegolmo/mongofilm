from pathlib import Path
from DbConnector import DbConnector
import time
import statistics

class DirectorQueryExecutor:
    def __init__(self):
        self.connection = DbConnector()
        self.db = self.connection.db
        
    def query_top_directors(self, min_movies=5, top_n=10):
      
        start_time = time.time()
        
        movies = list(self.db.movies.aggregate([
            {
                "$match": {
                    "crew": {"$exists": True, "$ne": None},
                    "revenue": {"$exists": True, "$ne": None},
                    "vote_average": {"$exists": True, "$ne": None}
                }
            },
            {
                "$project": {
                    "title": 1,
                    "revenue": 1,
                    "vote_average": 1,
                    "crew": 1
                }
            }
        ]))
        

        director_dict = {}
        for movie in movies:
            for member in movie['crew']:
                if member.get('job') == "Director":
                    director_name = member.get('name')
                    if director_name not in director_dict:
                        director_dict[director_name] = {
                            "movies": [],
                            "revenues": [],
                            "vote_averages": []
                        }
                    director_dict[director_name]["movies"].append(movie["title"])
                    director_dict[director_name]["revenues"].append(movie["revenue"])
                    director_dict[director_name]["vote_averages"].append(movie["vote_average"])
        
        print(f"   ⏳ Filtrando directores con ≥ {min_movies} películas...")
        results = []
        for director, data in director_dict.items():
            if len(data["movies"]) >= min_movies:
                median_revenue = statistics.median(data["revenues"])
                avg_vote = sum(data["vote_averages"]) / len(data["vote_averages"])
                results.append({
                    "director": director,
                    "movie_count": len(data["movies"]),
                    "median_revenue": median_revenue,
                    "mean_vote": round(avg_vote, 2)
                })
        
        results.sort(key=lambda x: x["median_revenue"], reverse=True)
        
        elapsed = time.time() - start_time
        
        for i, director in enumerate(results[:top_n], 1):
            print(f"{i}. {director['director']}")
            print(f"   • Películas: {director['movie_count']}")
            print(f"   • Mediana revenue: {director['median_revenue']}")
            print(f"   • Promedio vote_average: {director['mean_vote']:.2f}")
        
        return results
    
    def export_results_to_csv(self, results, output_path):
        import pandas as pd
        df = pd.DataFrame(results)
        df.to_csv(output_path, index=False)
    
    def close(self):
        self.connection.close_connection()

def main():
    executor = DirectorQueryExecutor()
    try:
        results = executor.query_top_directors(min_movies=5, top_n=10)
        output_path = Path(__file__).resolve().parent.parent / "results" / "top_directors.csv"
        output_path.parent.mkdir(exist_ok=True)
        executor.export_results_to_csv(results, output_path)
    finally:
        executor.close()

if __name__ == "__main__":
    main()

