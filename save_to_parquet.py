# stream_save_hf_to_parquet.py

from datasets import load_dataset
import pandas as pd
import os

# Create output dir
os.makedirs("parquet_chunks", exist_ok=True)

# Categories to process
categories = [
    "raw_review_Electronics",
    "raw_review_Home_and_Kitchen",
    "raw_review_Books"
]

chunk_size = 100_000  # Adjust based on RAM
num_chunks_per_category = 5  # 5 x 100k = 500k rows per category

for category in categories:
    print(f"\n📥 Streaming: {category}")
    stream = load_dataset("McAuley-Lab/Amazon-Reviews-2023", category, split="full", streaming=True)
    stream = iter(stream)   
    for i in range(num_chunks_per_category):
        print(f"➡️ Chunk {i+1}/{num_chunks_per_category}")
        data = [next(stream) for _ in range(chunk_size)]
        df = pd.DataFrame(data)
        
        file_path = f"parquet_chunks/{category}_chunk_{i}.parquet"
        df.to_parquet(file_path)
        print(f"✅ Saved: {file_path}")

