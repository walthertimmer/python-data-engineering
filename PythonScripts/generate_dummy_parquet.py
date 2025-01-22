import os
import polars as pl
import numpy as np
from datetime import datetime, timedelta

# Generate sample data
n_rows = 1000
np.random.seed(42)

# Create dummy data
data = {
    'id': pl.Series(range(1, n_rows + 1)),
    'name': pl.Series([f'Person_{i}' for i in range(n_rows)]),
    'age': pl.Series(np.random.randint(18, 80, n_rows)),
    'salary': pl.Series(np.random.uniform(30000, 120000, n_rows).round(2)),
    'department': pl.Series(np.random.choice(['HR', 'IT', 'Sales', 'Marketing', 'Engineering'], n_rows)),
    'hire_date': pl.Series([(datetime.now() - timedelta(days=np.random.randint(0, 1825))) for _ in range(n_rows)]),
    'active': pl.Series(np.random.choice([True, False], n_rows, p=[0.9, 0.1]))
}

# Create DataFrame
df = pl.DataFrame(data)

# Set up output path dynamically
current_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(os.path.dirname(current_dir), 'DummyFiles')
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, 'dummy.parquet')

# Save as Parquet file
df.write_parquet(output_file)

print(f"Dummy Parquet file created successfully at: {output_file}")