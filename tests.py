import os
import pandas as pd
import fastparquet
from append import write, _previous_date_offset

path = os.path.expanduser('~/Documents/code/draft/data/')
file = path + 'weather_data'

# Write first data. With this 1st DataFrame, 2 partitions are created (2 files):
#  - the 1st contains the 1st row.
#  - the 2nd contains the 2nd and 3rd rows. (they become sorted)
date_group_offset = '2H'
df1 = pd.DataFrame({'humidity': [0.3, 0.8, 0.9],
                    'pressure': [1e5, 1.1e5, 0.95e5],
                    'location': ['Paris', 'Paris', 'Milan']},
                    index = [pd.Timestamp('2020/01/02 01:59:00'),
                             pd.Timestamp('2020/01/02 03:59:00'),
                             pd.Timestamp('2020/01/02 02:59:00')])
write(file, df1, date_group_offset = date_group_offset)

# Check parquet file content.
first_offset = _previous_date_offset(df1.index[0], date_group_offset)
first_offset_as_ts = first_offset.timestamp()
last_offset_as_ts = _previous_date_offset(df1.index[-1], date_group_offset).timestamp()
df1_1_name = file + '/part.' + str(int(first_offset_as_ts)) + '.parquet'
df1_1 = pd.read_parquet(df1_1_name)
df1_2_name = file + '/part.' + str(int(last_offset_as_ts)) + '.parquet'
df1_2 = pd.read_parquet(df1_2_name)
offset_list = pd.period_range(start = first_offset, end = df1.index[-1], freq = date_group_offset)

# Check we can load with fastparquet (check that metadata have been successfully re-built).
fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()
fpt.statistics


# Merge 'new' data:
#   - from this step, we end up with 4 partitions (4 files), among which 2 are new partitions, and one is an overwritten partition.
#   - first row in new data is older than lattest data in existing dataset. A new partition is created.
#   - 2nd & 3rd rows have 'new values' and are added in the existing 1st partition file. It is overwritten.
#   - 4rth row has exactly the same data as a row already existing in the existing 2nd partition file: this file is left unmodified.
#   - 5th row is newer data, added in a new partition file.
df2 = pd.DataFrame({'humidity': [0.5, 0.3, 0.3, 0.8, 1.1],
                    'pressure': [9e4, 1e5, 1.1e5, 1.1e5, 0.95e5],
                    'location': ['Tokyo', 'Paris', 'Paris', 'Paris', 'Paris']},
                    index = [pd.Timestamp('2020/01/01 01:59:00'),
                             pd.Timestamp('2020/01/02 01:58:00'),
                             pd.Timestamp('2020/01/02 01:59:00'),
                             pd.Timestamp('2020/01/02 03:59:00'),
                             pd.Timestamp('2020/01/03 02:59:00')])
write(file, df2, date_group_offset = date_group_offset)

# Check parquet file content.
first_offset = _previous_date_offset(df2.index[0], date_group_offset)
first_offset_as_ts = first_offset.timestamp()
last_offset_as_ts = _previous_date_offset(df2.index[-1], date_group_offset).timestamp()
df2_1_name = file + '/part.' + str(int(first_offset_as_ts)) + '.parquet'
df2_1 = pd.read_parquet(df2_1_name)
df2_2_name = file + '/part.' + str(int(last_offset_as_ts)) + '.parquet'
df2_2 = pd.read_parquet(df2_2_name)

# Check we can load with fastparquet (check that metadata have been successfully re-built).
fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()
fpt.statistics


# Check drop_duplicate_on index (it works because index is nameless, so is called 'index' by default)
# Beware that in existing dataset, we have a duplicate index in 2nd partition file.
# Because we will add data to this partition, it will also be removed.
# We check that over the miscellaneous duplicates removed, it is the one added here that is kept (check on humidity value 0.4).
df3 = pd.DataFrame({'humidity': [0.4],
                    'pressure': [1e5],
                    'location': ['Paris']},
                    index = [pd.Timestamp('2020/01/02 01:59:00')])
write(file, df3, date_group_offset = date_group_offset, drop_duplicates_on = 'index')
fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()

#df2 = pd.read_parquet(file+'/part.1577923200.parquet')


# Check drop_duplicate_on a set of columns and the index.
# We check that the 3rd row that is kept has pressure value 1.1e5 (from new data) and not not 1e5 (from existing data)
# We also check that the 4th row (with same timestamp) is kept because humidity value is not the same.
# Humidity column is indeed used to check duplicates.
df4 = pd.DataFrame({'humidity': [0.4, 0.5],
                    'pressure': [1.1e5, 1.1e5],
                    'location': ['Paris', 'Paris']},
                    index = [pd.Timestamp('2020/01/02 01:59:00'),
                             pd.Timestamp('2020/01/02 01:59:00')])
write(file, df4, date_group_offset = date_group_offset, drop_duplicates_on = ['humidity', 'location'])
fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()


# Use case example: modification of a value before rewrite.
# Pressure probe at Paris got an offset error between time ts1 & ts2. Need to add a constant value to these rows.
fpt = fastparquet.ParquetFile(file)
ts1=pd.Timestamp('2020/01/02 1:59')
ts2=pd.Timestamp('2020/01/02 4:00')
df5 = fpt.to_pandas(filters=[('index', '>=', ts1), ('index', '<=', ts2)])

m = (df5.index >= ts1) & (df5.index <= ts2) & (df5['location'] == 'Paris')
df5.loc[m,'pressure'] = df5.loc[m,'pressure'] + 9e4
write(file, df5, date_group_offset = date_group_offset, drop_duplicates_on = ['humidity', 'location'])
fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()


########################################################################################################################
# All previous checks replayed once again, with an index name this time.
date_group_offset = '2H'
df1 = pd.DataFrame({'humidity': [0.3, 0.8, 0.9],
                    'pressure': [1e5, 1.1e5, 0.95e5],
                    'location': ['Paris', 'Paris', 'Milan']},
                    index = [pd.Timestamp('2020/01/02 01:59:00'),
                             pd.Timestamp('2020/01/02 03:59:00'),
                             pd.Timestamp('2020/01/02 02:59:00')])
df1.index.name = 'timestamp'
write(file, df1, date_group_offset = date_group_offset)
fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()

df2 = pd.DataFrame({'humidity': [0.5, 0.3, 0.3, 0.8, 1.1],
                    'pressure': [9e4, 1e5, 1.1e5, 1.1e5, 0.95e5],
                    'location':['Tokyo', 'Paris', 'Paris', 'Paris', 'Paris']},
                    index = [pd.Timestamp('2020/01/01 01:59:00'),
                             pd.Timestamp('2020/01/02 01:58:00'),
                             pd.Timestamp('2020/01/02 01:59:00'),
                             pd.Timestamp('2020/01/02 03:59:00'),
                             pd.Timestamp('2020/01/03 02:59:00')])
df2.index.name = 'timestamp'
write(file, df2, date_group_offset = date_group_offset)
fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()

df3 = pd.DataFrame({'humidity': [0.4],
                    'pressure': [1e5],
                    'location': ['Paris']},
                    index = [pd.Timestamp('2020/01/02 01:59:00')])
df3.index.name = 'timestamp'
write(file, df3, date_group_offset = date_group_offset, drop_duplicates_on = 'index')
fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()

df4 = pd.DataFrame({'humidity': [0.4, 0.5],
                    'pressure': [1.1e5, 1.1e5],
                    'location': ['Paris', 'Paris']},
                    index = [pd.Timestamp('2020/01/02 01:59:00'),
                             pd.Timestamp('2020/01/02 01:59:00')])
df4.index.name = 'timestamp'
write(file, df4, date_group_offset = date_group_offset, drop_duplicates_on = ['humidity', 'location'])
fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()


fpt = fastparquet.ParquetFile(file)
ts1=pd.Timestamp('2020/01/02 1:00')
ts2=pd.Timestamp('2020/01/02 4:00')
df5 = fpt.to_pandas(filters=[('index', '>=', ts1), ('index', '<=', ts2)])
m = (df5.index >= ts1) & (df5.index <= ts2) & (df5['location'] == 'Paris')
df5.loc[m,'pressure'] = df5.loc[m,'pressure'] + 9e4
write(file, df5, date_group_offset = date_group_offset, drop_duplicates_on = ['humidity', 'location'])
fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()


########################################################################################################################
# All previous checks replayed once again, with offset '1D'.
date_group_offset = '1D'
df1 = pd.DataFrame({'humidity': [0.3, 0.8, 0.9],
                    'pressure': [1e5, 1.1e5, 0.95e5],
                    'location': ['Paris', 'Paris', 'Milan']},
                    index = [pd.Timestamp('2020/01/02 01:59:00'),
                             pd.Timestamp('2020/01/03 03:59:00'),
                             pd.Timestamp('2020/01/03 02:59:00')])
write(file, df1, date_group_offset = date_group_offset)

# Check parquet file content.
first_offset = _previous_date_offset(df1.index[0], date_group_offset)
first_offset_as_ts = first_offset.timestamp()
last_offset_as_ts = _previous_date_offset(df1.index[-1], date_group_offset).timestamp()
df1_1_name = file + '/part.' + str(int(first_offset_as_ts)) + '.parquet'
df1_1 = pd.read_parquet(df1_1_name)
df1_2_name = file + '/part.' + str(int(last_offset_as_ts)) + '.parquet'
df1_2 = pd.read_parquet(df1_2_name)
offset_list = pd.period_range(start = first_offset, end = df1.index[-1], freq = date_group_offset)

# Merge 'new' data:
#   - from this step, we end up with 4 partitions (4 files), among which 2 are new partitions, and one is an overwritten partition.
#   - first row in new data is older than lattest data in existing dataset. A new partition is created.
#   - 2nd & 3rd rows have 'new values' and are added in the existing 1st partition file. It is overwritten.
#   - 4rth row has exactly the same data as a row already existing in the existing 2nd partition file: this file is left unmodified.
#   - 5th row is newer data, added in a new partition file.
df2 = pd.DataFrame({'humidity': [0.5, 0.3, 0.3, 0.8, 1.1],
                    'pressure': [9e4, 1e5, 1.1e5, 1.1e5, 0.95e5],
                    'location': ['Tokyo', 'Paris', 'Paris', 'Paris', 'Paris']},
                    index = [pd.Timestamp('2020/01/01 01:59:00'),
                             pd.Timestamp('2020/01/02 01:58:00'),
                             pd.Timestamp('2020/01/02 01:59:00'),
                             pd.Timestamp('2020/01/03 03:59:00'),
                             pd.Timestamp('2020/01/04 02:59:00')])
write(file, df2, date_group_offset = date_group_offset)

first_offset = _previous_date_offset(df2.index[0], date_group_offset)
first_offset_as_ts = first_offset.timestamp()
last_offset_as_ts = _previous_date_offset(df2.index[-1], date_group_offset).timestamp()
df2_1_name = file + '/part.' + str(int(first_offset_as_ts)) + '.parquet'
df2_1 = pd.read_parquet(df2_1_name)
df2_2_name = file + '/part.' + str(int(last_offset_as_ts)) + '.parquet'
df2_2 = pd.read_parquet(df2_2_name)

# Check drop_duplicate_on index
# Beware that in existing dataset, we have a duplicate index in 2nd partition file.
# Because we will add data to this partition, it will also be removed.
# We check that over the miscellaneous duplicates removed, it is the one added here that is kept (check on humidity value 0.4).
df3 = pd.DataFrame({'humidity': [0.4],
                    'pressure': [1e5],
                    'location': ['Paris']},
                    index = [pd.Timestamp('2020/01/02 01:59:00')])
write(file, df3, date_group_offset = date_group_offset, drop_duplicates_on = 'index')
fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()

# Use case example: modification of a value before rewrite.
# Pressure probe at Paris got an offset error between time ts1 & ts2. Need to add a constant value to these rows.
fpt = fastparquet.ParquetFile(file)
ts1=pd.Timestamp('2020/01/02 1:59')
ts2=pd.Timestamp('2020/01/03 4:00')
df5 = fpt.to_pandas(filters=[('index', '>=', ts1), ('index', '<=', ts2)])

m = (df5.index >= ts1) & (df5.index <= ts2) & (df5['location'] == 'Paris')
df5.loc[m,'pressure'] = df5.loc[m,'pressure'] + 9e4
write(file, df5, date_group_offset = date_group_offset, drop_duplicates_on = ['humidity', 'location'])
fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()


########################################################################################################################
# All previous checks replayed once again, with offset '1M'.
date_group_offset = 'M'
df1 = pd.DataFrame({'humidity': [0.3, 0.8, 0.9],
                    'pressure': [1e5, 1.1e5, 0.95e5],
                    'location': ['Paris', 'Paris', 'Milan']},
                    index = [pd.Timestamp('2020/01/02 01:59:00'),
                             pd.Timestamp('2020/02/03 03:59:00'),
                             pd.Timestamp('2020/02/03 02:59:00')])
write(file, df1, date_group_offset = date_group_offset)

# Check parquet file content.
first_offset = _previous_date_offset(df1.index[0], date_group_offset)
first_offset_as_ts = first_offset.timestamp()
last_offset_as_ts = _previous_date_offset(df1.index[-1], date_group_offset).timestamp()
df1_1_name = file + '/part.' + str(int(first_offset_as_ts)) + '.parquet'
df1_1 = pd.read_parquet(df1_1_name)
df1_2_name = file + '/part.' + str(int(last_offset_as_ts)) + '.parquet'
df1_2 = pd.read_parquet(df1_2_name)
offset_list = pd.period_range(start = first_offset, end = df1.index[-1], freq = date_group_offset)

# Merge 'new' data:
#   - from this step, we end up with 4 partitions (4 files), among which 2 are new partitions, and one is an overwritten partition.
#   - first row in new data is older than lattest data in existing dataset. A new partition is created.
#   - 2nd & 3rd rows have 'new values' and are added in the existing 1st partition file. It is overwritten.
#   - 4rth row has exactly the same data as a row already existing in the existing 2nd partition file: this file is left unmodified.
#   - 5th row is newer data, added in a new partition file.
df2 = pd.DataFrame({'humidity': [0.5, 0.3, 0.3, 0.8, 1.1],
                    'pressure': [9e4, 1e5, 1.1e5, 1.1e5, 0.95e5],
                    'location': ['Tokyo', 'Paris', 'Paris', 'Paris', 'Paris']},
                    index = [pd.Timestamp('2019/12/01 01:59:00'),
                             pd.Timestamp('2020/01/02 01:58:00'),
                             pd.Timestamp('2020/01/02 01:59:00'),
                             pd.Timestamp('2020/02/03 03:59:00'),
                             pd.Timestamp('2020/03/03 02:59:00')])
write(file, df2, date_group_offset = date_group_offset)

first_offset = _previous_date_offset(df2.index[0], date_group_offset)
first_offset_as_ts = first_offset.timestamp()
last_offset_as_ts = _previous_date_offset(df2.index[-1], date_group_offset).timestamp()
df2_1_name = file + '/part.' + str(int(first_offset_as_ts)) + '.parquet'
df2_1 = pd.read_parquet(df2_1_name)
df2_2_name = file + '/part.' + str(int(last_offset_as_ts)) + '.parquet'
df2_2 = pd.read_parquet(df2_2_name)

fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()

# Check drop_duplicate_on index
# Beware that in existing dataset, we have a duplicate index in 2nd partition file.
# Because we will add data to this partition, it will also be removed.
# We check that over the miscellaneous duplicates removed, it is the one added here that is kept (check on humidity value 0.4).
df3 = pd.DataFrame({'humidity': [0.4],
                    'pressure': [1e5],
                    'location': ['Paris']},
                    index = [pd.Timestamp('2020/01/02 01:59:00')])
write(file, df3, date_group_offset = date_group_offset, drop_duplicates_on = 'index')
fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()

# Use case example: modification of a value before rewrite.
# Pressure probe at Paris got an offset error between time ts1 & ts2. Need to add a constant value to these rows.
fpt = fastparquet.ParquetFile(file)
ts1=pd.Timestamp('2020/01/02 1:59')
ts2=pd.Timestamp('2020/02/03 4:00')
df5 = fpt.to_pandas(filters=[('index', '>=', ts1), ('index', '<=', ts2)])

m = (df5.index >= ts1) & (df5.index <= ts2) & (df5['location'] == 'Paris')
df5.loc[m,'pressure'] = df5.loc[m,'pressure'] + 9e4
write(file, df5, date_group_offset = date_group_offset, drop_duplicates_on = ['humidity', 'location'])
fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()


########################################################################################################################
# All previous checks replayed once again, with offset '1W'.
date_group_offset = '1W'
df1 = pd.DataFrame({'humidity': [0.3, 0.8, 0.9],
                    'pressure': [1e5, 1.1e5, 0.95e5],
                    'location': ['Paris', 'Paris', 'Milan']},
                    index = [pd.Timestamp('2020/01/02 01:59:00'),
                             pd.Timestamp('2020/02/03 03:59:00'),
                             pd.Timestamp('2020/02/03 02:59:00')])
write(file, df1, date_group_offset = date_group_offset)

# Check parquet file content.
first_offset = _previous_date_offset(df1.index[0], date_group_offset)
first_offset_as_ts = first_offset.timestamp()
last_offset_as_ts = _previous_date_offset(df1.index[-1], date_group_offset).timestamp()
df1_1_name = file + '/part.' + str(int(first_offset_as_ts)) + '.parquet'
df1_1 = pd.read_parquet(df1_1_name)
df1_2_name = file + '/part.' + str(int(last_offset_as_ts)) + '.parquet'
df1_2 = pd.read_parquet(df1_2_name)
offset_list = pd.period_range(start = first_offset, end = df1.index[-1], freq = date_group_offset)

# Merge 'new' data:
#   - from this step, we end up with 4 partitions (4 files), among which 2 are new partitions, and one is an overwritten partition.
#   - first row in new data is older than lattest data in existing dataset. A new partition is created.
#   - 2nd & 3rd rows have 'new values' and are added in the existing 1st partition file. It is overwritten.
#   - 4rth row has exactly the same data as a row already existing in the existing 2nd partition file: this file is left unmodified.
#   - 5th row is newer data, added in a new partition file.
df2 = pd.DataFrame({'humidity': [0.5, 0.3, 0.3, 0.8, 1.1],
                    'pressure': [9e4, 1e5, 1.1e5, 1.1e5, 0.95e5],
                    'location': ['Tokyo', 'Paris', 'Paris', 'Paris', 'Paris']},
                    index = [pd.Timestamp('2019/12/01 01:59:00'),
                             pd.Timestamp('2020/01/02 01:58:00'),
                             pd.Timestamp('2020/01/02 01:59:00'),
                             pd.Timestamp('2020/02/03 03:59:00'),
                             pd.Timestamp('2020/03/03 02:59:00')])
write(file, df2, date_group_offset = date_group_offset)

first_offset = _previous_date_offset(df2.index[0], date_group_offset)
first_offset_as_ts = first_offset.timestamp()
last_offset_as_ts = _previous_date_offset(df2.index[-1], date_group_offset).timestamp()
df2_1_name = file + '/part.' + str(int(first_offset_as_ts)) + '.parquet'
df2_1 = pd.read_parquet(df2_1_name)
df2_2_name = file + '/part.' + str(int(last_offset_as_ts)) + '.parquet'
df2_2 = pd.read_parquet(df2_2_name)

fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()

# Check drop_duplicate_on index
# Beware that in existing dataset, we have a duplicate index in 2nd partition file.
# Because we will add data to this partition, it will also be removed.
# We check that over the miscellaneous duplicates removed, it is the one added here that is kept (check on humidity value 0.4).
df3 = pd.DataFrame({'humidity': [0.4],
                    'pressure': [1e5],
                    'location': ['Paris']},
                    index = [pd.Timestamp('2020/01/02 01:59:00')])
write(file, df3, date_group_offset = date_group_offset, drop_duplicates_on = 'index')
fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()

# Use case example: modification of a value before rewrite.
# Pressure probe at Paris got an offset error between time ts1 & ts2. Need to add a constant value to these rows.
fpt = fastparquet.ParquetFile(file)
ts1=pd.Timestamp('2020/01/02 1:59')
ts2=pd.Timestamp('2020/02/03 4:00')
df5 = fpt.to_pandas(filters=[('index', '>=', ts1), ('index', '<=', ts2)])

m = (df5.index >= ts1) & (df5.index <= ts2) & (df5['location'] == 'Paris')
df5.loc[m,'pressure'] = df5.loc[m,'pressure'] + 9e4
write(file, df5, date_group_offset = date_group_offset, drop_duplicates_on = ['humidity', 'location'])
fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()


########################################################################################################################
# All previous checks replayed once again, with offset '1W', and date-like date in column 'timestamp'.
date_group_offset = '1W'
df1 = pd.DataFrame({'humidity': [0.3, 0.8, 0.9],
                    'pressure': [1e5, 1.1e5, 0.95e5],
                    'location': ['Paris', 'Paris', 'Milan'],
                    'timestamp': [pd.Timestamp('2020/01/02 01:59:00'),
                                  pd.Timestamp('2020/02/03 03:59:00'),
                                  pd.Timestamp('2020/02/03 02:59:00')]})
write(file, df1, date_group_offset = date_group_offset, group_on = 'timestamp')

# Check parquet file content.
first_offset = _previous_date_offset(df1['timestamp'][0], date_group_offset)
first_offset_as_ts = first_offset.timestamp()
last_offset_as_ts = _previous_date_offset(df1['timestamp'].iloc[-1], date_group_offset).timestamp()
df1_1_name = file + '/part.' + str(int(first_offset_as_ts)) + '.parquet'
df1_1 = pd.read_parquet(df1_1_name)
df1_2_name = file + '/part.' + str(int(last_offset_as_ts)) + '.parquet'
df1_2 = pd.read_parquet(df1_2_name)
offset_list = pd.period_range(start = first_offset, end = df1['timestamp'].iloc[-1], freq = date_group_offset)

# Merge 'new' data:
#   - from this step, we end up with 4 partitions (4 files), among which 2 are new partitions, and one is an overwritten partition.
#   - first row in new data is older than lattest data in existing dataset. A new partition is created.
#   - 2nd & 3rd rows have 'new values' and are added in the existing 1st partition file. It is overwritten.
#   - 4rth row has exactly the same data as a row already existing in the existing 2nd partition file: this file is left unmodified.
#   - 5th row is newer data, added in a new partition file.
df2 = pd.DataFrame({'humidity': [0.5, 0.3, 0.3, 0.8, 1.1],
                    'pressure': [9e4, 1e5, 1.1e5, 1.1e5, 0.95e5],
                    'location': ['Tokyo', 'Paris', 'Paris', 'Paris', 'Paris'],
                    'timestamp': [pd.Timestamp('2019/12/01 01:59:00'),
                                  pd.Timestamp('2020/01/02 01:58:00'),
                                  pd.Timestamp('2020/01/02 01:59:00'),
                                  pd.Timestamp('2020/02/03 03:59:00'),
                                  pd.Timestamp('2020/03/03 02:59:00')]})
write(file, df2, date_group_offset = date_group_offset, group_on = 'timestamp')

fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()

# Check drop_duplicate_on index
# Beware that in existing dataset, we have a duplicate index in 2nd partition file.
# Because we will add data to this partition, it will also be removed.
# We check that over the miscellaneous duplicates removed, it is the one added here that is kept (check on humidity value 0.4).
df3 = pd.DataFrame({'humidity': [0.4],
                    'pressure': [1e5],
                    'location': ['Paris'],
                    'timestamp': [pd.Timestamp('2020/01/02 01:59:00')]})
write(file, df3, date_group_offset = date_group_offset, drop_duplicates_on = 'timestamp', group_on = 'timestamp')
fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()

# Use case example: modification of a value before rewrite.
# Pressure probe at Paris got an offset error between time ts1 & ts2. Need to add a constant value to these rows.
fpt = fastparquet.ParquetFile(file)
ts1=pd.Timestamp('2020/01/02 1:59')
ts2=pd.Timestamp('2020/02/03 4:00')
df5 = fpt.to_pandas(filters=[('timestamp', '>=', ts1), ('timestamp', '<=', ts2)])

m = (df5['timestamp'] >= ts1) & (df5['timestamp'] <= ts2) & (df5['location'] == 'Paris')
df5.loc[m,'pressure'] = df5.loc[m,'pressure'] + 9e4
write(file, df5, date_group_offset = date_group_offset, drop_duplicates_on = ['humidity', 'location'], group_on = 'timestamp')
fpt = fastparquet.ParquetFile(file)
fpt.to_pandas()


########################################################################################################################
# Speed test 'pd.merge' vs 'pd.concat'.
date_group_offset = '1W'

datetime_index = pd.date_range(start = pd.Timestamp('2020/01/02 01:00:00'), end = pd.Timestamp('2020/02/02 12:00:00'), freq='10T')

df1 = pd.DataFrame({'humidity': [1] * len(datetime_index),
                    'pressure': [1] * len(datetime_index),
                    'location': [1] * len(datetime_index)},
                    index = datetime_index)
write(file, df1, date_group_offset = date_group_offset)

datetime_index = pd.date_range(start = pd.Timestamp('2020/01/02 01:00:00'), end = pd.Timestamp('2020/02/02 12:00:00'), freq='10T')
df2 = pd.DataFrame({'humidity': [2] * len(datetime_index),
                    'pressure': [2] * len(datetime_index),
                    'location': [2] * len(datetime_index)},
                    index = datetime_index)

%timeit write(file, df2, date_group_offset = date_group_offset)
# with merge
#62.6 ms ± 1.42 ms per loop (mean ± std. dev. of 7 runs, 10 loops each)
# with concat.drop_duplicates
#61.2 ms ± 1.61 ms per loop (mean ± std. dev. of 7 runs, 10 loops each)


########################################################################################################################
# All previous checks replayed once again, with a PeriodIndex.
date_group_offset = '4H'
period_index = pd.period_range(start = pd.Timestamp('2020/01/02 01:00:00'), end = pd.Timestamp('2020/01/02 12:00:00'), freq='2H')
df1 = pd.DataFrame({'humidity': [0.3, 0.8, 0.9, 0.3, 0.8, 0.9],
                    'pressure': [1e5, 1.1e5, 0.95e5, 1e5, 1.1e5, 0.95e5],
                    'location': ['Paris', 'Paris', 'Milan', 'Paris', 'Paris', 'Milan'],
                    'period': period_index})
write(file, df1, date_group_offset = date_group_offset, group_on = 'period')





########################################################################################################################
# All previous checks replayed once again, with a PeriodIndex.
# /!\ Does not work - see https://github.com/dask/fastparquet/issues/543 /!\
date_group_offset = '4H'

datetime_index = pd.date_range(start = pd.Timestamp('2020/01/02 01:00:00'), end = pd.Timestamp('2020/01/02 12:00:00'), freq='2H')
df1 = pd.DataFrame({'humidity': [0.3, 0.8, 0.9, 0.3, 0.8, 0.9],
                    'pressure': [1e5, 1.1e5, 0.95e5, 1e5, 1.1e5, 0.95e5],
                    'location': ['Paris', 'Paris', 'Milan', 'Paris', 'Paris', 'Milan']},
                    index = datetime_index)
fastparquet.write(file, df1)


period_index = pd.period_range(start = pd.Timestamp('2020/01/02 01:00:00'), end = pd.Timestamp('2020/01/02 12:00:00'), freq='2H')
df1 = pd.DataFrame({'humidity': [0.3, 0.8, 0.9, 0.3, 0.8, 0.9],
                    'pressure': [1e5, 1.1e5, 0.95e5, 1e5, 1.1e5, 0.95e5],
                    'location': ['Paris', 'Paris', 'Milan', 'Paris', 'Paris', 'Milan']},
                    index = period_index)
fastparquet.write(file, df1)

write(file, df1, date_group_offset = date_group_offset)


