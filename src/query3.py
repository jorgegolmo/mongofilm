from pathlib import Path
from DbConnector import DbConnector
import time

class MovieQueryExecutor:
    def __init__(self):
        print("🔌 Conectando a MongoDB...")
        self.connection = DbConnector()
        self.db = self.connection.db
        
    def query_top_actors_by_genre_breadth(self, min_movies=10, top_n=10, example_genres=5):
        """
        Lista los top N actores con mayor amplitud de géneros.
        
        Args:
            min_movies: Número mínimo de películas acreditadas (default: 10)
            top_n: Número de actores a retornar (default: 10)
            example_genres: Número de géneros de ejemplo (default: 5)
        """
        print(f"\n🎬 Ejecutando query: Top {top_n} actores por amplitud de géneros")
        print(f"   📋 Filtro: Actores con ≥ {min_movies} películas acreditadas")
        print("-" * 70)
        
        start_time = time.time()
        
        pipeline = [
            # 1. Desempaquetar el array de cast
            {
                "$unwind": {
                    "path": "$cast",
                    "preserveNullAndEmptyArrays": False
                }
            },
            
            # 2. Desempaquetar el array de genres
            {
                "$unwind": {
                    "path": "$genres",
                    "preserveNullAndEmptyArrays": False
                }
            },
            
            # 3. Agrupar por actor para contar películas y géneros distintos
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
            
            # 4. Calcular el número de géneros distintos
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
            
            # 5. Filtrar actores con al menos min_movies películas
            {
                "$match": {
                    "movie_count": {"$gte": min_movies}
                }
            },
            
            # 6. Ordenar por número de géneros distintos (descendente)
            {
                "$sort": {
                    "genre_count": -1,
                    "actor_name": 1
                }
            },
            
            # 7. Limitar a top_n resultados
            {
                "$limit": top_n
            },
            
            # 8. Limitar los géneros de ejemplo
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
        
        # Ejecutar aggregation
        results = list(self.db.movies.aggregate(pipeline))
        
        elapsed = time.time() - start_time
        
        # Mostrar resultados
        print(f"\n✅ Query ejecutada en {elapsed:.2f}s")
        print(f"📊 Resultados encontrados: {len(results)}")
        print("\n" + "="*70)
        print("TOP ACTORES POR AMPLITUD DE GÉNEROS")
        print("="*70)
        
        for i, actor in enumerate(results, 1):
            print(f"\n{i}. {actor['actor_name']}")
            print(f"   • Actor ID: {actor['actor_id']}")
            print(f"   • Géneros distintos: {actor['genre_count']}")
            print(f"   • Películas acreditadas: {actor['movie_count']}")
            print(f"   • Géneros de ejemplo: {', '.join(actor['example_genres'])}")
            
            # Mostrar todos los géneros si quieres ver el detalle completo
            # print(f"   • Todos los géneros: {', '.join(actor['all_genres'])}")
        
        print("\n" + "="*70)
        
        return results
    
    def export_results_to_csv(self, results, output_path):
        """Exporta los resultados a CSV"""
        import pandas as pd
        
        # Preparar datos para CSV
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
        print(f"\n💾 Resultados exportados a: {output_path}")
    
    def close(self):
        """Cierra la conexión"""
        self.connection.close_connection()

def main():
    executor = MovieQueryExecutor()
    
    try:
        # Ejecutar la query
        results = executor.query_top_actors_by_genre_breadth(
            min_movies=10,
            top_n=10,
            example_genres=5
        )
        
        # Opcional: Exportar a CSV
        output_path = Path(__file__).resolve().parent.parent / "results" / "top_actors_genre_breadth.csv"
        output_path.parent.mkdir(exist_ok=True)
        executor.export_results_to_csv(results, output_path)
        
    finally:
        executor.close()

if __name__ == "__main__":
    main()
