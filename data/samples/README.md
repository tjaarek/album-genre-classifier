# Sample covers

5 cover images per genre (50 total, ~6 MB), sampled from `data/albums_clean.csv`
with `numpy.random.default_rng(seed=42)`.

These are committed for **reproducibility testing**: anyone who clones the
repository can verify the full pipeline (load → preprocess → train inference)
on these 50 images without re-running the Spotify scrape.

To regenerate the full dataset (2125 covers from 260 artists), open and run
`notebooks/01_data_collection.ipynb` end-to-end. Requires Spotify Developer
credentials in `.env`.
