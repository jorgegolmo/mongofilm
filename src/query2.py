from pathlib import Path
from DbConnector import DbConnector
import time

class MovieQueryExecutor:
    def __init__(self):
        print("🔌 Conectando a MongoDB...")
        self.connection = DbConnector()
        self.db = self.connection.db
        
    def query_actor_pairs_costarring(self, min_movies=3, limit=20):
        """
        Lista pares de actores que han co-actuado en ≥ min_movies películas.
        
        Args:
            min_movies: Número mínimo de películas juntos (default: 3)
            limit: Número máximo de resultados a mostrar (default: 20)
        """
        print(f"\n🎭 Buscando pares de actores con co-actuaciones")
        print(f"   📋 Filtro: ≥ {min_movies} películas juntos")
        print("-" * 80)
        
        start_time = time.time()
        
        # Obtener películas con cast y vote_average
        print("   ⏳ Cargando películas desde MongoDB...")
        movies_with_actors = list(self.db.movies.aggregate([
            {
                "$match": {
                    "cast": {"$exists": True, "$ne": None, "$not": {"$size": 0}},
                    "vote_average": {"$exists": True, "$ne": None}
                }
            },
            {
                "$project": {
                    "tmdbId": 1,
                    "title": 1,
                    "vote_average": 1,
                    "cast.id": 1,
                    "cast.name": 1
                }
            }
        ]))
        
        print(f"   ✓ {len(movies_with_actors):,} películas cargadas")
        
        # Generar pares de actores
        print("   ⏳ Generando pares de actores...")
        actor_pairs = {}
        
        for movie in movies_with_actors:
            if not movie.get('cast') or len(movie['cast']) < 2:
                continue
                
            actors = movie['cast']
            vote_avg = movie.get('vote_average', 0)
            title = movie.get('title', 'Unknown')
            
            # Crear todos los pares posibles en esta película
            for i in range(len(actors)):
                for j in range(i + 1, len(actors)):
                    actor1 = actors[i]
                    actor2 = actors[j]
                    
                    # Ordenar por ID para evitar duplicados (A,B) vs (B,A)
                    if actor1['id'] > actor2['id']:
                        actor1, actor2 = actor2, actor1
                    
                    pair_key = (actor1['id'], actor2['id'])
                    
                    # Inicializar o actualizar el par
                    if pair_key not in actor_pairs:
                        actor_pairs[pair_key] = {
                            'actor1_id': actor1['id'],
                            'actor1_name': actor1['name'],
                            'actor2_id': actor2['id'],
                            'actor2_name': actor2['name'],
                            'movies': [],
                            'vote_averages': []
                        }
                    
                    actor_pairs[pair_key]['movies'].append(title)
                    actor_pairs[pair_key]['vote_averages'].append(vote_avg)
        
        print(f"   ✓ {len(actor_pairs):,} pares únicos generados")
        
        # Filtrar y calcular estadísticas
        print(f"   ⏳ Filtrando pares con ≥ {min_movies} películas...")
        results = []
        
        for pair_key, pair_data in actor_pairs.items():
            co_appearances = len(pair_data['movies'])
            
            if co_appearances >= min_movies:
                # Calcular promedio de vote_average
                avg_vote = sum(pair_data['vote_averages']) / len(pair_data['vote_averages'])
                
                results.append({
                    'actor1_id': pair_data['actor1_id'],
                    'actor1_name': pair_data['actor1_name'],
                    'actor2_id': pair_data['actor2_id'],
                    'actor2_name': pair_data['actor2_name'],
                    'co_appearances': co_appearances,
                    'average_vote': round(avg_vote, 2),
                    'example_movies': pair_data['movies'][:5]
                })
        
        print(f"   ✓ {len(results):,} pares filtrados")
        
        # Ordenar por número de co-apariciones (descendente)
        results.sort(key=lambda x: (-x['co_appearances'], x['actor1_name']))
        
        # Limitar resultados
        display_results = results[:limit]
        
        elapsed = time.time() - start_time
        
        # Mostrar resultados
        print(f"\n✅ Query ejecutada en {elapsed:.2f}s")
        print(f"📊 Total de pares encontrados: {len(results):,}")
        print(f"📋 Mostrando top {len(display_results)}")
        print("\n" + "="*80)
        print("PARES DE ACTORES CON MÁS CO-ACTUACIONES")
        print("="*80)
        
        for i, pair in enumerate(display_results, 1):
            print(f"\n{i}. {pair['actor1_name']} & {pair['actor2_name']}")
            print(f"   • Co-apariciones: {pair['co_appearances']} películas")
            print(f"   • Promedio vote_average: {pair['average_vote']:.2f}")
            print(f"   • Películas ejemplo: {', '.join(pair['example_movies'][:3])}")
        
        print("\n" + "="*80)
        
        return results
    
    def export_results_to_csv(self, results, output_path):
        """Exporta los resultados a CSV"""
        import pandas as pd
        
        data = []
        for pair in results:
            data.append({
                'actor1_id': pair['actor1_id'],
                'actor1_name': pair['actor1_name'],
                'actor2_id': pair['actor2_id'],
                'actor2_name': pair['actor2_name'],
                'co_appearances': pair['co_appearances'],
                'average_vote': pair['average_vote'],
                'example_movies': ', '.join(pair['example_movies'][:5])
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
        # Ejecutar query
        results = executor.query_actor_pairs_costarring(
            min_movies=3,
            limit=20  # Top 20 pares
        )
        
        # Exportar a CSV
        output_path = Path(__file__).resolve().parent.parent / "results" / "actor_pairs_costarring.csv"
        output_path.parent.mkdir(exist_ok=True)
        executor.export_results_to_csv(results, output_path)
        
    finally:
        executor.close()

if __name__ == "__main__":
    main()
