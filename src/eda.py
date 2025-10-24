# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
# ---

# %% [markdown]
# This notebook contains the Exploratory Data Analysis for Exercise 3 of TDT4225 - Autumn 2025/2026.
#
# In this EDA we will analyze and clean the CSVs provided by the professors, to obtain data we can insert into the MongoDB database and work with. First, we will takinga  look at an overview of the data.

# %%
from pathlib import Path
import polars as pl

pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_rows(-1)
pl.Config.set_fmt_str_lengths(1024)

dat = Path(__file__).resolve().parent.parent / "dat"

csvs = {}
csvs["movies"] = pl.read_csv(dat / "origin" / "movies_metadata.csv", ignore_errors = True) # Clean up invalid values
csvs["credits"] = pl.read_csv(dat / "origin" / "credits.csv")
csvs["keywords"] = pl.read_csv(dat / "origin" / "keywords.csv")
csvs["ratings"] = pl.read_csv(dat / "origin" / "ratings.csv")
csvs["links"] = pl.read_csv(dat / "origin" / "links.csv")

def summary(df):
    print(pl.DataFrame({
        "column": df.columns,
        "type": [dtype for dtype in df.dtypes],
        "length": [df.height for col in df.columns],
        "nulls": [df[col].null_count() for col in df.columns],
        "uniques": [df[col].n_unique() for col in df.columns],
        "repeats": [df[col].count() - df[col].n_unique() for col in df.columns],
        "min": [df[col].min() for col in df.columns],
        "max": [df[col].max() for col in df.columns],
        "head": [df[col].first() for col in df.columns]
    }, strict=False))
    print()


for file in csvs.keys():
    print("----- " + file + " -----")
    summary(csvs[file])

# %% [markdown]
# We can see that the data is varied and large so, for the sake of simplicity, we will go CSV by CSV analyzing and cleaning it. Let's start with the movie metadata.

# %%
summary(csvs["movies"])

# %% [markdown]
#
# # Metadata
#
# The metadata CSV contains, as its name says. The movie metadata. It's first field is whether the film is an adult one. It contains null values, but otherwise is just True/False (the number of unique values being three takes into account the null ones). We have decided to remove the films with null values so as to assure the quality of the data.
#
# A similar phenomenon occurs with the bugdet. It's an integer field which contains null values, which we shall clean for the same reason. The same case occurs with the original_language, overview, popularity, revenue, runtime, status, title, video, vote_average and vote_count fields.
#
# In addition, by looking at the numeric fields, we can disclose that the values are adequate (no negative budget, no negative vote count vote_average goes from 0-10, etc.). We also check that status does not have an unknown option that we could default the nulls to and found no data to clean in original_title.

# %%
csvs["movies"] = csvs["movies"].drop_nulls(["adult", "budget", "original_language", "overview", "popularity", "revenue", "runtime", "status", "title", "video", "vote_average", "vote_count"])
summary(csvs["movies"])
print(csvs["movies"]["status"].unique())

# %% [markdown]
# In case of id and imdb_id, we will also remove repeated values, along with the null ones.
#
# Though similar, in title and poster_path we decided to keep repeated entries because, while not common, it could be the case that two different movies have the same title or image.

# %%
csvs["movies"] = csvs["movies"].drop_nulls(["imdb_id"]).unique(subset=["id"]).unique(subset=["imdb_id"])
summary(csvs["movies"])

# %% [markdown]
# Next up is release_date, which will have its data changed to datetime as we deem it more appropiate. In this conversion we will also drop invalid dates and null entries.

# %%
csvs["movies"] = csvs["movies"].with_columns(pl.col("release_date").str.strptime(pl.Date, format="%Y-%m-%d", strict=False)).drop_nulls(["release_date"])
summary(csvs["movies"])

# %% [markdown]
# In the case of belongs_to_collection, homepage and tagline we will leave them nullable as the sheer amount of null values would make deleting them a bad choice. In addition, poster_path will also be nullable for consistency, as in the JSON fields (examinated later) its amount of null values also makes leaving the field as is the better option. Furthermore, the homepages are always valid HTTP/HTTPS URLs.

# %% [markdown]
# The only fields remaining are the ones formatted as JSON lists. For pruning them, we will first remove invalid JSON and check the values.

# %%
from ast import literal_eval as parse

def is_json(string):
    try:
        return isinstance(parse(string), (list, dict))
    except Exception:
        return False

json_cols = [
    "belongs_to_collection",
    "genres",
    "production_companies",
    "production_countries",
    "spoken_languages"
]

for col in json_cols:
    csvs["movies"] = csvs["movies"].filter(
        pl.col(col).is_null() |
        pl.col(col).map_elements(is_json, return_dtype=pl.Boolean)
    )
    print(csvs["movies"][col].drop_nulls().head(1))
summary(csvs["movies"])

# %% [markdown]
# We have discerned that belongs_to_collection is a list of (id, name, poster_path, backdrop_path), genres is a list of (id, name), production_companies is a list of (name, id), production companies is a list of (iso_3166_1, name) and spoken_languages is another list of (iso_639_1, name). The next step is finding out if the JSON data needs cleaning.

# %%
from json import dumps
for col in json_cols:
    print("----- " + col + " -----")
    data = []
    for item in csvs["movies"][col].drop_nulls():
        json = parse(item)
        if isinstance(json, dict):
            data.append(json)
        else:
            data.extend(json)
    summary(pl.DataFrame(list({dumps(item, sort_keys=True): item for item in data}.values())))


# %% [markdown]
# We can see, however, that the data seems to already be in order: the IDs are always positive if numeric (and seem to be valid ISO codes otherwise) and are never repeated, and there are no null values except for the image paths.

# %% [markdown]
# # Credits

# %%
summary(csvs["credits"])

# %% [markdown]
# In this case, there are not null values and all the IDs are positive, but we need to eliminate the repeats.

# %%
csvs["credits"] = csvs["credits"].unique(subset=["id"])
summary(csvs["credits"])

# %% [markdown]
# Now let us do the same procedure for the JSON fields as with the previous CSV.

# %%
json_cols = [
    "cast",
    "crew"
]

for col in json_cols:
    csvs["credits"] = csvs["credits"].filter(
        pl.col(col).is_null() |
        pl.col(col).map_elements(is_json, return_dtype=pl.Boolean)
    )
summary(csvs["credits"])

for col in json_cols:
    print("----- " + col + " -----")
    data = []
    for item in csvs["credits"][col].drop_nulls():
        json = parse(item)
        if isinstance(json, dict):
            data.append(json)
        else:
            data.extend(json)
    summary(pl.DataFrame(list({dumps(item, sort_keys=True): item for item in data}.values())))

# %% [markdown]
# All of the null data are image paths, so we will leave them as is like their previous counterparts. On the other hand, we see that the IDs, while taking positive values, are repeated, but the primary key seems to be id-credit_id in both cases, and since credit_id has no repeats, the entries can still be uniquely identified.

# %% [markdown]
# # Keywords

# %%
summary(csvs["keywords"])

# %% [markdown]
# The keywords CSV is very similar, so we will continue by doing the same procedure

# %%
csvs["keywords"] = csvs["keywords"].unique(subset=["id"])

csvs["keywords"] = csvs["keywords"].filter(
    pl.col("keywords").is_null() |
    pl.col("keywords").map_elements(is_json, return_dtype=pl.Boolean)
)
summary(csvs["keywords"])

print("----- keywords -----")
data = []
for item in csvs["keywords"]["keywords"].drop_nulls():
    json = parse(item)
    if isinstance(json, dict):
        data.append(json)
    else:
        data.extend(json)
summary(pl.DataFrame(list({dumps(item, sort_keys=True): item for item in data}.values())))

# %% [markdown]
# The JSON data is already clean. There are no null values and the IDs are unique positive integers.

# %% [markdown]
# # Ratings

# %%
summary(csvs["ratings"])

# %% [markdown]
# We can see that this dataset contains a large amount of data, but it seems to be of good quality. There are no null values and all of the numbers seem to be appropiate (all of them positive, ratings from 0 to 5...) but the IDs are repeated. We will not remove entries indiscriminately, though, as the unique identifier for each entry is userId - movieId. We have to check if that subset is repeated.

# %%
csvs["ratings"] = csvs["ratings"].unique(subset=["userId", "movieId"])
summary(csvs["ratings"])

# %% [markdown]
# No changes happened, so this data was already clean. As there are no JSON fields, the analysis of this CSV is done.

# %% [markdown]
# # Links

# %%
summary(csvs["links"])

# %% [markdown]
# There are some null values, so we will delete those entries. We will also delete repeated TMDB IDs.
#
# csvs["links"] = csvs["links"].drop_nulls()
# csvs["links"] = csvs["links"].unique(subset=["tmdbId"])
# summary(csvs["links"])

# %% [markdown]
# Each CSV contains only clean data now, but since credits and links both point to movie IDs, we will delete the entries inside of them that point to non-existent IDs and viceversa.

# %%

csvs["movies"] = csvs["movies"].join(csvs["credits"].select("id"), on="id", how="semi").join(csvs["links"].select(pl.col("tmdbId").alias("id")), on="id", how="semi")
csvs["credits"] = csvs["credits"].join(csvs["movies"].select("id"), on="id", how="semi")
csvs["links"] = csvs["links"].join(csvs["movies"].select(pl.col("id").alias("tmdbId")), on="tmdbId", how="semi")

for file in ["movies", "credits", "links"]:
    print("----- " + file + " -----")
    summary(csvs[file])

# %% [markdown]
# Also, ratings' ID points to the movieID and keywords' ID points to tmdbID, so we will remove both ratings and keywords which point to non-existent movies.

# %%
csvs["ratings"] = csvs["ratings"].join(csvs["links"].select("movieId"), on="movieId", how="semi")
csvs["keywords"] = csvs["keywords"].join(csvs["links"].select(pl.col("tmdbId").alias("id")), on="id", how="semi")
summary(csvs["ratings"])
summary(csvs["keywords"])

# %% [markdown]
# Finally, we will write the datasets to disk in new, clean CSVs

# %%
from os import makedirs

makedirs(dat / "clean", exist_ok=True)
for file in csvs.keys():
    print("----- " + file + " -----")
    summary(csvs[file])
    csvs[file].write_csv(dat / "clean" / (file + ".csv"))
