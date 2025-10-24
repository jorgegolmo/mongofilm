from pathlib import Path
from DbConnector import DbConnector
import pandas as pd
from ast import literal_eval
import time

class MovieInserter:
    def __init__(self):
        print("🔌 Conectando a MongoDB...")
        self.connection = DbConnector()
        self.db = self.connection.db
        self.batch_size = 1000
        self.chunk_size = 5000
        
    def to_json(self, x):
        """Convierte string JSON a objeto Python"""
        try:
            return literal_eval(x) if pd.notnull(x) else None
        except:
            return None
    
    def insert_batch(self, collection_name, records, start_idx=0):
        """Inserta registros en batches con contador"""
        total = len(records)
        inserted = 0
        
        for i in range(0, total, self.batch_size):
            batch = records[i:i+self.batch_size]
            self.db[collection_name].insert_many(batch)
            inserted += len(batch)
            print(f"  ✓ {start_idx + inserted:,}/{start_idx + total:,}", end='\r', flush=True)
        
        print(f"  ✓ {start_idx + inserted:,}/{start_idx + total:,}")
        return inserted
    
    def insert_movies(self, movies_path, credits_path, keywords_path):
        """Inserta movies fusionando con credits y keywords"""
        print("\n📂 Leyendo archivos...")
        start_time = time.time()
        
        # Leer movies
        print("  📄 Leyendo movies.csv...")
        movies = pd.read_csv(movies_path)
        print(f"  ✓ {len(movies):,} películas leídas")
        
        # Leer credits
        print("  📄 Leyendo credits.csv...")
        credits = pd.read_csv(credits_path)
        credits["cast"] = credits["cast"].apply(self.to_json)
        credits["crew"] = credits["crew"].apply(self.to_json)
        print(f"  ✓ {len(credits):,} credits leídos")
        
        # Leer keywords
        print("  📄 Leyendo keywords.csv...")
        keywords = pd.read_csv(keywords_path)
        keywords["keywords"] = keywords["keywords"].apply(self.to_json)
        print(f"  ✓ {len(keywords):,} keywords leídos")
        
        # Convertir campos JSON de movies
        print("\n🔄 Convirtiendo campos JSON de movies...")
        json_cols = ["genres", "production_companies", "production_countries",
                     "spoken_languages", "belongs_to_collection"]
        
        for col in json_cols:
            if col in movies.columns:
                movies[col] = movies[col].apply(self.to_json)
        
        # Fusionar todo
        print("\n🔗 Fusionando datos...")
        merged = movies.merge(credits, on="id", how="left") \
                       .merge(keywords, on="id", how="left")
        
        # Renombrar id a tmdbId para claridad
        merged = merged.rename(columns={'id': 'tmdbId'})
        print(f"  ✓ Datos fusionados: {len(merged):,} documentos")
        
        # Insertar en MongoDB
        print("\n💾 Insertando películas en MongoDB...")
        movies_records = merged.to_dict(orient="records")
        total_inserted = self.insert_batch("movies", movies_records, 0)
        
        elapsed = time.time() - start_time
        print(f"\n✅ Movies insertadas: {total_inserted:,} documentos en {elapsed:.2f}s")
        
        # Liberar memoria
        del merged, movies, credits, keywords, movies_records
        return total_inserted
    
    def insert_ratings(self, ratings_path, links_path):
        """Inserta ratings con tmdbId mapeado desde links"""
        print("\n📂 Procesando ratings...")
        start_time = time.time()
        
        # Cargar mapeo de links
        print("  📄 Cargando mapeo movieId → tmdbId...")
        links = pd.read_csv(links_path, usecols=["movieId", "tmdbId"])
        movieLens_to_tmdb = dict(zip(links['movieId'], links['tmdbId']))
        print(f"  ✓ {len(movieLens_to_tmdb):,} mapeos cargados")
        del links
        
        # Procesar ratings por chunks
        print("\n💾 Insertando ratings por chunks...")
        total_inserted = 0
        
        for chunk in pd.read_csv(ratings_path, chunksize=self.chunk_size):
            # Agregar tmdbId usando el mapeo
            chunk['tmdbId'] = chunk['movieId'].map(movieLens_to_tmdb)
            
            records = chunk.to_dict(orient="records")
            
            # Insertar batch
            for i in range(0, len(records), self.batch_size):
                batch = records[i:i+self.batch_size]
                self.db.ratings.insert_many(batch)
                total_inserted += len(batch)
                print(f"  ✓ {total_inserted:,} ratings insertados", end='\r', flush=True)
        
        print(f"  ✓ {total_inserted:,} ratings insertados")
        
        elapsed = time.time() - start_time
        print(f"\n✅ Ratings insertados: {total_inserted:,} documentos en {elapsed:.2f}s")
        return total_inserted
    
    def create_indexes(self):
        """Crea índices para optimizar queries"""
        print("\n🔍 Creando índices...")
        start_time = time.time()
        
        # Índice para movies
        print("  • Creando índice: movies.tmdbId")
        self.db.movies.create_index("tmdbId", unique=True)
        
        # Índices para ratings
        print("  • Creando índice: ratings.tmdbId")
        self.db.ratings.create_index("tmdbId")
        
        print("  • Creando índice: ratings.userId")
        self.db.ratings.create_index("userId")
        
        print("  • Creando índice: ratings.movieId")
        self.db.ratings.create_index("movieId")
        
        # Índice compuesto para queries comunes
        print("  • Creando índice compuesto: ratings.tmdbId + rating")
        self.db.ratings.create_index([("tmdbId", 1), ("rating", -1)])
        
        elapsed = time.time() - start_time
        print(f"✅ Índices creados en {elapsed:.2f}s")
    
    def verify_insertion(self):
        """Verifica que los datos se insertaron correctamente"""
        print("\n📊 Verificando inserción...")
        
        # Contar documentos
        movies_count = self.db.movies.count_documents({})
        ratings_count = self.db.ratings.count_documents({})
        
        print(f"\n  📦 Colecciones insertadas:")
        print(f"    • movies:  {movies_count:,} documentos")
        print(f"    • ratings: {ratings_count:,} documentos")
        
        # Verificar documento de ejemplo
        sample_movie = self.db.movies.find_one(
            {"cast": {"$exists": True, "$ne": None}},
            {"title": 1, "tmdbId": 1, "cast": {"$slice": 2}, "_id": 0}
        )
        
        if sample_movie:
            print(f"\n  🎬 Ejemplo de película:")
            print(f"    • Título: {sample_movie.get('title')}")
            print(f"    • tmdbId: {sample_movie.get('tmdbId')}")
            if sample_movie.get('cast'):
                print(f"    • Cast (primeros 2): {len(sample_movie['cast'])} actores")
        
        # Verificar ratings con tmdbId
        sample_rating = self.db.ratings.find_one(
            {"tmdbId": {"$exists": True}},
            {"userId": 1, "movieId": 1, "tmdbId": 1, "rating": 1, "_id": 0}
        )
        
        if sample_rating:
            print(f"\n  ⭐ Ejemplo de rating:")
            print(f"    • userId: {sample_rating.get('userId')}")
            print(f"    • movieId (MovieLens): {sample_rating.get('movieId')}")
            print(f"    • tmdbId: {sample_rating.get('tmdbId')}")
            print(f"    • rating: {sample_rating.get('rating')}")
        
        # Verificar integridad de relaciones
        ratings_with_tmdb = self.db.ratings.count_documents({"tmdbId": {"$exists": True, "$ne": None}})
        coverage = (ratings_with_tmdb / ratings_count * 100) if ratings_count > 0 else 0
        
        print(f"\n  🔗 Integridad de relaciones:")
        print(f"    • Ratings con tmdbId: {ratings_with_tmdb:,} ({coverage:.2f}%)")
        
        return {
            "movies": movies_count,
            "ratings": ratings_count,
            "ratings_with_tmdb": ratings_with_tmdb,
            "coverage": coverage
        }
    
    def run(self, data_path):
        """Ejecuta todo el proceso de inserción - SOLO 2 COLECCIONES"""
        print("="*70)
        print("🚀 INICIANDO INSERCIÓN MASIVA EN MONGODB")
        print("   📊 Estrategia: 2 colecciones (movies + ratings)")
        print("="*70)
        
        total_start = time.time()
        
        try:
            # 1. Insertar movies (con credits y keywords embebidos)
            movies_count = self.insert_movies(
                data_path / "movies.csv",
                data_path / "credits.csv",
                data_path / "keywords.csv"
            )
            
            # 2. Insertar ratings (con tmdbId para JOIN)
            ratings_count = self.insert_ratings(
                data_path / "ratings.csv",
                data_path / "links.csv"
            )
            
            # 3. Crear índices para optimizar queries
            self.create_indexes()
            
            # 4. Verificar inserción
            stats = self.verify_insertion()
            
            # Resumen final
            total_elapsed = time.time() - total_start
            total_docs = movies_count + ratings_count
            
            print("\n" + "="*70)
            print("🎉 INSERCIÓN COMPLETADA EXITOSAMENTE")
            print("="*70)
            print(f"  ⏱️  Tiempo total: {total_elapsed:.2f}s ({total_elapsed/60:.2f} min)")
            print(f"  📦 Total documentos: {total_docs:,}")
            print(f"  ⚡ Velocidad promedio: {total_docs/total_elapsed:.0f} docs/s")
            print(f"\n  📊 Desglose:")
            print(f"    • Movies:  {movies_count:,} documentos")
            print(f"    • Ratings: {ratings_count:,} documentos")
            print(f"\n  🔗 Cobertura de relaciones: {stats['coverage']:.2f}%")
            print("="*70)
            
        except Exception as e:
            print(f"\n❌ ERROR durante la inserción: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def close(self):
        """Cierra la conexión"""
        self.connection.close_connection()

def main():
    # Ruta a los datos limpios
    data_path = Path(__file__).resolve().parent.parent / "dat" / "clean"
    
    inserter = MovieInserter()
    try:
        inserter.run(data_path)
    finally:
        inserter.close()

if __name__ == "__main__":
    main()
