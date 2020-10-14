import pandas as pd
import numpy as np
import os
import pytest
import tempfile


df = pd.DataFrame({
    'a': np.random.rand(100),
    'b': np.random.randint(100),
    'c': np.random.choice(['a', 'b', 'c', 'd'], size=100)
})
df.index.name = 'p'
df2 = pd.DataFrame({
    'd': np.random.rand(100),
    'e': np.random.randint(100),
    'f': np.random.choice(['a', 'b', 'c', 'd'], size=100)
})


