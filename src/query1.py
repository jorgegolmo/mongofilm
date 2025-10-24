from pathlib import Path
from DbConnector import DbConnector
import time
import statistics

class DirectorQueryExecutor:
    def __init__(self):
        print("🔌 Conectando a MongoDB...")
        self.connection = DbConnector()
        self.db = self.connection.db
        
    def query_top_directors(self, min_movies=5, top_n=10):
        """
        Obtiene los 10 directores con más mediana de revenue y al menos min_movies películas.
        Reporta también cantidad de películas y promedio de vote_average.
        """
        print(f"\n🎬 Buscando directores con ≥ {min_movies} películas")
        print("-" * 80)
        
        start_time = time.time()
        
        # Cargar todas las películas con crew
        print("   ⏳ Cargando películas desde MongoDB...")
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
        
        print(f"   ✓ {len(movies):,} películas cargadas")
        
        # Filtrar directores y agrupar por nombre
        print("   ⏳ Agrupando películas por director...")
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
        
        # Filtrar directores con ≥ min_movies
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
        
        # Ordenar por mediana de revenue descendente
        results.sort(key=lambda x: x["median_revenue"], reverse=True)
        
        elapsed = time.time() - start_time
        
        # Mostrar resultados
        print(f"\n✅ Query ejecutada en {elapsed:.2f}s")
        print(f"📋 Mostrando top {top_n} directores por mediana de revenue\n")
        print("="*80)
        for i, director in enumerate(results[:top_n], 1):
            print(f"{i}. {director['director']}")
            print(f"   • Películas: {director['movie_count']}")
            print(f"   • Mediana revenue: {director['median_revenue']}")
            print(f"   • Promedio vote_average: {director['mean_vote']:.2f}")
        
        print("="*80)
        return results
    
    def export_results_to_csv(self, results, output_path):
        import pandas as pd
        df = pd.DataFrame(results)
        df.to_csv(output_path, index=False)
        print(f"\n💾 Resultados exportados a: {output_path}")
    
    def close(self):
        """Cierra la conexión"""
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

