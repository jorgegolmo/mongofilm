from pathlib import Path
from DbConnector import DbConnector
import pandas as pd
from ast import literal_eval
import time

class MovieInserter:
    def __init__(self):
        print("Conecting to MongoDB...")
        self.connection = DbConnector()
        self.db = self.connection.db
        self.batch_size = 1000
        self.chunk_size = 5000
        
    def to_json(self, x):
        try:
            return literal_eval(x) if pd.notnull(x) else None
        except:
            return None
    
    def insert_batch(self, collection_name, records, start_idx=0):
        total = len(records)
        inserted = 0
        
        for i in range(0, total, self.batch_size):
            batch = records[i:i+self.batch_size]
            self.db[collection_name].insert_many(batch)
            inserted += len(batch)
            print(f"{start_idx + inserted:,}/{start_idx + total:,}", end='\r', flush=True)
        
        print(f"{start_idx + inserted:,}/{start_idx + total:,}")
        return inserted
    
    def insert_movies(self, movies_path, credits_path, keywords_path):
        print("\nReading files")
        start_time = time.time()
        
        movies = pd.read_csv(movies_path)
        print(f"{len(movies):,} películas leídas")
        
        credits = pd.read_csv(credits_path)
        credits["cast"] = credits["cast"].apply(self.to_json)
        credits["crew"] = credits["crew"].apply(self.to_json)
        print(f"{len(credits):,} credits leídos")
        
        keywords = pd.read_csv(keywords_path)
        keywords["keywords"] = keywords["keywords"].apply(self.to_json)
        print(f"{len(keywords):,} keywords leídos")
        
        json_cols = ["genres", "production_companies", "production_countries",
                     "spoken_languages", "belongs_to_collection"]
        
        for col in json_cols:
            if col in movies.columns:
                movies[col] = movies[col].apply(self.to_json)
        
        merged = movies.merge(credits, on="id", how="left") \
                       .merge(keywords, on="id", how="left")
        
        merged = merged.rename(columns={'id': 'tmdbId'})
        print(f"Merged data: {len(merged):,} documentos")
        
        print("\nInsert in MongoDB...")
        movies_records = merged.to_dict(orient="records")
        total_inserted = self.insert_batch("movies", movies_records, 0)
        
        elapsed = time.time() - start_time
        print(f"\niNSERTED: {total_inserted:,} documents in {elapsed:.2f}s")
        
        # Liberar memoria
        del merged, movies, credits, keywords, movies_records
        return total_inserted
    
    def insert_ratings(self, ratings_path, links_path):
        start_time = time.time()
        
        links = pd.read_csv(links_path, usecols=["movieId", "tmdbId"])
        movieLens_to_tmdb = dict(zip(links['movieId'], links['tmdbId']))
        print(f"{len(movieLens_to_tmdb):,} mapping charged")
        del links
        
        print("\nInserting ratings")
        total_inserted = 0
        
        for chunk in pd.read_csv(ratings_path, chunksize=self.chunk_size):
            chunk['tmdbId'] = chunk['movieId'].map(movieLens_to_tmdb)
            
            records = chunk.to_dict(orient="records")
            
            for i in range(0, len(records), self.batch_size):
                batch = records[i:i+self.batch_size]
                self.db.ratings.insert_many(batch)
                total_inserted += len(batch)
                print(f"{total_inserted:,} ratings inserted", end='\r', flush=True)
        
        print(f"{total_inserted:,} ratings inserted")
        
        elapsed = time.time() - start_time
        print(f"\nRatings inserted: {total_inserted:,} documents in {elapsed:.2f}s")
        return total_inserted
    
    def create_indexes(self):
        start_time = time.time()
        
        self.db.movies.create_index("tmdbId", unique=True)
        
        self.db.ratings.create_index("tmdbId")
        
        self.db.ratings.create_index("userId")
        
        self.db.ratings.create_index("movieId")
        
        self.db.ratings.create_index([("tmdbId", 1), ("rating", -1)])
        
        elapsed = time.time() - start_time
        print(f"Index created in {elapsed:.2f}s")
    
    def verify_insertion(self):
        
        movies_count = self.db.movies.count_documents({})
        ratings_count = self.db.ratings.count_documents({})

        sample_movie = self.db.movies.find_one(
            {"cast": {"$exists": True, "$ne": None}},
            {"title": 1, "tmdbId": 1, "cast": {"$slice": 2}, "_id": 0}
        )
        
        if sample_movie:
            print(f"\nMovie example:")
            print(f"    • Tittle: {sample_movie.get('title')}")
            print(f"    • tmdbId: {sample_movie.get('tmdbId')}")
            if sample_movie.get('cast'):
                print(f"    • Cast (first 2): {len(sample_movie['cast'])} actors")
        
        sample_rating = self.db.ratings.find_one(
            {"tmdbId": {"$exists": True}},
            {"userId": 1, "movieId": 1, "tmdbId": 1, "rating": 1, "_id": 0}
        )
        
        if sample_rating:
            print(f"\nRating example:")
            print(f"    • userId: {sample_rating.get('userId')}")
            print(f"    • movieId (MovieLens): {sample_rating.get('movieId')}")
            print(f"    • tmdbId: {sample_rating.get('tmdbId')}")
            print(f"    • rating: {sample_rating.get('rating')}")
        
        ratings_with_tmdb = self.db.ratings.count_documents({"tmdbId": {"$exists": True, "$ne": None}})
        coverage = (ratings_with_tmdb / ratings_count * 100) if ratings_count > 0 else 0
        
        print(f"\nIntegrity of relations:")
        print(f"    • Ratings with tmdbId: {ratings_with_tmdb:,} ({coverage:.2f}%)")
        
        return {
            "movies": movies_count,
            "ratings": ratings_count,
            "ratings_with_tmdb": ratings_with_tmdb,
            "coverage": coverage
        }
    
    def run(self, data_path):
        print("Inserting data")
        total_start = time.time()
        
        try:
            movies_count = self.insert_movies(
                data_path / "movies.csv",
                data_path / "credits.csv",
                data_path / "keywords.csv"
            )
            
            ratings_count = self.insert_ratings(
                data_path / "ratings.csv",
                data_path / "links.csv"
            )
            
            self.create_indexes()
            
            stats = self.verify_insertion()
            
            total_elapsed = time.time() - total_start
            total_docs = movies_count + ratings_count
            

            
        except Exception as e:
            import traceback
            traceback.print_exc()
            raise
    
    def close(self):
        self.connection.close_connection()

def main():
    data_path = Path(__file__).resolve().parent.parent / "dat" / "clean"
    
    inserter = MovieInserter()
    try:
        inserter.run(data_path)
    finally:
        inserter.close()

if __name__ == "__main__":
    main()
