# fastparquet_append
Append function for Parquet dataset through fastparquet lib.

## Walk through

* Create a 1st DataFrame and initiate a Parquet Dataset.
```python
import os
import pandas as pd
import fastparquet
from append import write, _previous_offset

path = os.path.expanduser('~/Documents/code/draft/data/')
file = path + 'weather_data'

index_offsets = '2H'
df1 = pd.DataFrame({'humidity': [0.3, 0.8, 0.9],
                    'pressure': [1e5, 1.1e5, 0.95e5],
                    'location':['Paris', 'Paris', 'Milan']},
                    index = [pd.Timestamp('2020/01/02 01:59:00'),
                             pd.Timestamp('2020/01/02 03:59:00'),
                             pd.Timestamp('2020/01/02 02:59:00')])
write(file, df1, index_offsets = index_offsets)
```

```bash
ls -l weather_data
  910 déc.  30 22:53 _common_metadata
 1664 déc.  30 22:53 _metadata
 1613 déc.  30 22:53 part.1577923200.parquet
 1646 déc.  30 22:53 part.1577930400.parquet
```
```python
fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()

                     humidity  pressure location
index                                           
2020-01-02 01:59:00       0.3  100000.0    Paris
2020-01-02 02:59:00       0.9   95000.0    Milan
2020-01-02 03:59:00       0.8  110000.0    Paris
```

