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
df1

                     humidity  pressure location
2020-01-02 01:59:00       0.3  100000.0    Paris
2020-01-02 03:59:00       0.8  110000.0    Paris
2020-01-02 02:59:00       0.9   95000.0    Milan
```

```python
write(file, df1, index_offsets = index_offsets)
```

* Considering DateOffset of 2 hours, only 2 partition files are written.

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

* Merge 'new' data:
  - from this step, we end up with 4 partition files, among which 2 are new partitions, and one is an overwritten partition.
  - first row in new data is older than lattest data in existing dataset. A new partition is created.
  - 2nd & 3rd rows have 'new values' and are added in the existing 1st partition file. It is overwritten.
  - 4th row has exactly the same data as a row already in the existing 2nd partition file: this file is left unmodified.
  - 5th row is newer data, and it is added in a new partition file.

```python
df2 = pd.DataFrame({'humidity': [0.5, 0.3, 0.3, 0.8, 1.1],
                    'pressure': [9e4, 1e5, 1.1e5, 1.1e5, 0.95e5],
                    'location':['Tokyo', 'Paris', 'Paris', 'Paris', 'Paris']},
                    index = [pd.Timestamp('2020/01/01 01:59:00'),
                             pd.Timestamp('2020/01/02 01:58:00'),
                             pd.Timestamp('2020/01/02 01:59:00'),
                             pd.Timestamp('2020/01/02 03:59:00'),
                             pd.Timestamp('2020/01/03 02:59:00')])
write(file, df2, index_offsets = index_offsets)
df2

                     humidity  pressure location
2020-01-01 01:59:00       0.5   90000.0    Tokyo
2020-01-02 01:58:00       0.3  100000.0    Paris
2020-01-02 01:59:00       0.3  110000.0    Paris
2020-01-02 03:59:00       0.8  110000.0    Paris
2020-01-03 02:59:00       1.1   95000.0    Paris
```

* We can check that we have now 4 parquet files, one of them has not been modified since 1st step.

```bash
ls -l weather_data
ls -l weather_data/
total 24
  910 déc.  30 23:05 _common_metadata
 2418 déc.  30 23:05 _metadata
 1613 déc.  30 23:05 part.1577836800.parquet
 1679 déc.  30 23:05 part.1577923200.parquet
 1646 déc.  30 22:53 part.1577930400.parquet
 1613 déc.  30 23:05 part.1578016800.parquet
 ```
 
 ```python
fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()

                     humidity  pressure location
index                                           
2020-01-01 01:59:00       0.5   90000.0    Tokyo
2020-01-02 01:58:00       0.3  100000.0    Paris
2020-01-02 01:59:00       0.3  100000.0    Paris
2020-01-02 01:59:00       0.3  110000.0    Paris
2020-01-02 02:59:00       0.9   95000.0    Milan
2020-01-02 03:59:00       0.8  110000.0    Paris
2020-01-03 02:59:00       1.1   95000.0    Paris
```

* By default, duplicated rows are identified to be so based on index and all columns, and are dropped. Only index can be used.
  Beware that in existing dataset, there are duplicate indices in the 2nd partition file.
  Because data is added in this partition, duplicates rows there will be removed.
  Finally, let's highlight that over the rows removed, it is the one added last that is kept (check on humidity value 0.4).

```python
df3 = pd.DataFrame({'humidity': [0.4],
                    'pressure': [1e5],
                    'location':['Paris']},
                    index = [pd.Timestamp('2020/01/02 01:59:00')])
write(file, df3, index_offsets = index_offsets, drop_duplicates_on = 'index')
df3

                     humidity  pressure location
2020-01-02 01:59:00       0.4  100000.0    Paris
```

```python
fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()

                     humidity  pressure location
index                                           
2020-01-01 01:59:00       0.5   90000.0    Tokyo
2020-01-02 01:58:00       0.3  100000.0    Paris
2020-01-02 01:59:00       0.4  100000.0    Paris
2020-01-02 02:59:00       0.9   95000.0    Milan
2020-01-02 03:59:00       0.8  110000.0    Paris
2020-01-03 02:59:00       1.1   95000.0    Paris
```

* As a last use case, let say a pressure probe in Paris got an offset error between time ts1 & ts2. We need to fix the records by adding a constant value of 9e4 to these rows. With this `append` mode, we can do so by only modifying files in which we need to.

```python
# Load only required data.
fpt = fastparquet.ParquetFile(file)
ts1=pd.Timestamp('2020/01/02 1:00')
ts2=pd.Timestamp('2020/01/02 4:00')
df5 = fpt.to_pandas(filters=[('index', '>=', ts1), ('index', '<=', ts2)])

# Modify pressure values that need to be modified.
m = (df5.index >= ts1) & (df5.index <= ts2) & (df5['location'] == 'Paris')
df5.loc[m,'pressure'] = df5.loc[m,'pressure'] + 9e4

# Update data in Parquet dataset.
write(file, df5, index_offsets = index_offsets, drop_duplicates_on = ['humidity', 'location'])
fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()

                     humidity  pressure location
index                                           
2020-01-01 01:59:00       0.5   90000.0    Tokyo
2020-01-02 01:58:00       0.3  190000.0    Paris
2020-01-02 01:59:00       0.4  190000.0    Paris
2020-01-02 02:59:00       0.9   95000.0    Milan
2020-01-02 03:59:00       0.8  200000.0    Paris
2020-01-03 02:59:00       1.1   95000.0    Paris
```

* Over the xxx To of recorded data, the only partitions rewritten have been those overlapping in time with [ts1, ts2] range.

```bash
ls -l weather_data/
total 24
  910 déc.  30 23:16 _common_metadata
 2418 déc.  30 23:16 _metadata
 1613 déc.  30 23:05 part.1577836800.parquet
 1646 déc.  30 23:16 part.1577923200.parquet
 1646 déc.  30 23:16 part.1577930400.parquet
 1613 déc.  30 23:05 part.1578016800.parquet
```
