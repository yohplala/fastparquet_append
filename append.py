from os import path, scandir, mkdir
import pandas as pd
import fastparquet
from fastparquet.writer import merge

class WriteException(Exception):
    def __init__(self, message):
        self.message = message

def _previous_offset(time_marker, offset: pd.DateOffset) -> pd.Timestamp:
    """

    Parameters
      * time_marker (pd.Timestamp or pd.Period)
          Time marker from which is assessed closest earlier on offset timestamp.
      * offset (pd.DateOffset)
          Offset according pandas library, see https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects

    Returns
      * pd.Timestamp
          Closest earlier timestamp on offset from input timestamp considering midnight anchoring.

    """

    if isinstance(time_marker, pd.Timestamp):
        time_marker = pd.Period(time_marker, freq = offset.freqstr)
    start_time = time_marker.start_time
    end_time = time_marker.end_time
    if end_time - start_time < pd.Timedelta('1D'):
        midnight = start_time.normalize()
        n=(start_time-midnight)//offset
        return midnight + n*offset
    else:
        return offset.rollback(start_time).normalize()

def _merge(df1, df2, drop_duplicates_on=None) -> pd.DataFrame:
    """

    In case of duplicate rows (depending strategy retained to define what are duplicate rows), newer values are kept.
    (from newer DataFrame `df2`)

    Parameters
      * df1 (pd.DataFrame)
          First (legacy) DataFrame.
      * df2 (pd.DataFrame)
          Second (new) DataFrame.
      * drop_duplicates_on (optional)
          Strategy to consider rows as duplicates to be dropped. The default is None.
          * If 'None', index and all columns are considered to identify duplicates.
          * If 'index', only index is used to identify duplicates.
          * If a list of column names (with/without index name), only these subset of columns with index are used to identify duplicates.

    Returns
      * pd.DataFrame
          Concatenated DataFrame.

    """

    if not drop_duplicates_on:
        # Case duplicates identified based on index + all columns
        df1 = df1.reset_index()
        return df1.merge(df2.reset_index(), how='outer', on=list(df1.columns)).set_index(df1.columns[0]).sort_index()
    else:
        combined = pd.concat([df1, df2])
        if drop_duplicates_on == 'index':
            # Case duplicates identified based on index + all columns
            return combined.loc[~combined.index.duplicated(keep='last')].sort_index()
        elif isinstance(drop_duplicates_on, list):
            combined = combined.reset_index()
            if not combined.columns[0] in drop_duplicates_on:
                # Add index if not in the list.
                drop_duplicates_on.append(combined.columns[0])
            if all(item in combined.columns for item in drop_duplicates_on):
                # Check all requested column names are existing.
                return combined.drop_duplicates(subset=drop_duplicates_on, keep='last').set_index(combined.columns[0]).sort_index()
            else:
                raise WriteException('At least on value in {!s} is not a column/index name as in {!s}.'.format(str(drop_duplicates_on), str(list(combined.columns))))
        else:
            raise WriteException('{!s} is not a valid value for drop_duplicates_on parameter.'.format(drop_duplicates_on))            

def write(dir_name: str, data: pd.DataFrame, append: bool = True, index_offsets: str = None,
          drop_duplicates_on: str = None):
    """

    Implementation to merge data to an existing parquet Dataset, using Datetime partitioning.

    Features and limitations:
      * one row group per parquet file.
      * index does not need to be sorted. It will be sorted in this function.
      * feature: partitions are limited by time period (`pandas.Period`)
      * feature: all `pandas.Period` are anchored to midnight
      * feature: parquet file names embed period start time (rounded to seconds).
        This allows user to merge data that is actually older than already recorded data.
      * feature: different merging scenarii (Check use cases)
          * to append 'new' data that can be older or newer in time versus existing data.
          * or to correct existing data.
      * limitation: once `index_offsets` is defined for the 1st write, it should be always the same at each new data merging.
      * limitation: `index_offsets` only work with DataFrame having a `pandas.DatetimeIndex` or `pandas.PeriodIndex`
      * limitation: index_offset should not be lower than a second (because parquet filenames are rounded to the second)

    Parameters
      * dir_name (str)
          Output directory name.
      * data (pd.DataFrame
          New data to append to existing Parquet dataset.
      * index_offsets (str, optional)
          Date offset to be used to anchor row groups / parquet files (one row group per parquet file).
          The default is None. Provided date offset is automatically anchored to midnight.
      * append (bool, optional)
          Requ`ired to be set on `True` to update existing parquet files when partitioning is based on `index_offsets`.
          The default is True.
      * drop_duplicates_on (str or list, optional)
          Either None, 'index', or a list specifying names of the columns to be used to identify duplicates. Index is necessarily added to this list.
          If None, both index and all columns are used to identify duplicates.
          Dropping duplicates on columns only without the Datetime data used for partitioning is not within the scope of the 'append' mode.
          Duplicates indeed are only searched within the same partition, that is defined according the Datetime data.
          If a duplicated row is identified so only based on column values, it could be on a different partition than the one of the new data.
          For this latter case, the 'overwrite' mode has then to be implemented.


    ToDo.
      * how storing date_offset information in metadata, so that it is re-used when merging new data? 
        (user should not have to specify at each new merging, as once set, this data has not to be modified)

    Further improvement that could be considered.
      * let user specify a column name for operating the partition, not forcing it to be the index?
      * change 'append' parameter into 'write_mode' with values None, 'append' or 'overwrite'
          * with 'append', user can only add new data or modify existing data, keeping at least the same Datetime-based index
          * overwrite can only be used in case of Datetime partitioning. In this case, the input data fully replace data of the partition(s) it overlaps.
            (Can be used to remove data from existing partitions)
      * metadata is re-created from scratch. This should/could be optimized?
      * (not willing to delve into that) allow partitioning on float data instead of necessarily DateTimeIndex? (some complexity: offset definition with anchoring is to be defined)


    Close look on function interface:
        Current interface:
           file_scheme: 'simple' or 'hive'
           partition_on: [] columns name to split data in directories, if file_scheme is 'hive'
           row_group_offsets: int defining approximate number of rows in a partition file
           append: bool defining if new row groups have to be appended to existing row groups
       
        To create partitioned files with approximately same number of rows per file, in a single directory
           file_scheme: 'hive'
           row_group_offsets: int
           partition_on: None
           append: True

        Proposed changes for appending while considering DatetimeIndex partitioning
           file_scheme = 'hive'
           index_offsets = str or pd.DateOffset
           partition_on = [] kept as is
           append: True / or write_mode = 'append'

    """

    # Check df has a DatetimeIndex
    if not (isinstance(data.index, pd.DatetimeIndex) or isinstance(data.index, pd.PeriodIndex)):
        raise WriteException('DataFrame index has to be a DatetimeIndex or PeriodIndex for use with index_offsets.')
        
    if not isinstance(index_offsets, pd.DateOffset):
        index_offsets = pd.tseries.frequencies.to_offset(index_offsets)
    
    # /!\ Check to be implemented by reading from _metadata:
    # If appending, check existing target parquet dataset has same `index_offsets`, e.g '2H'...
    # Can it be store/read from metadata?
    # When appending, does not need to be provided again by the user: a single ìndex_offsets` is imposed.

    # 1st draft implementation, not using any _metadata, first to explain the idea.
    # Generate list of timestamps on offset to split the data 
    data = data.sort_index()
    start_time = _previous_offset(data.index[0], index_offsets)
    offset_list = pd.period_range(start = start_time, end = data.index[-1], freq = index_offsets)

    # If appending, and files already exist.
    if append and path.exists(dir_name) and [f for f in scandir(dir_name) if f.name[-8:] == '.parquet'] != []:
        fptr = fastparquet.ParquetFile(dir_name)
        # Index name is in 1st position.
        index_name = fptr.columns[0]
        
        # Setup iteration over offset list
        for offset in offset_list:
            # Load existing data if any
            start = offset.start_time
            end = offset.end_time
            existing = fptr.to_pandas(filters=[(index_name, '>=', start), (index_name, '<=', end)])
            new_slice = data.loc[(data.index >= start) & (data.index <= end)]
            if not (existing.empty or new_slice.empty):
                # Both DataFrames exist, merge them.
                new_slice = _merge(existing, new_slice, drop_duplicates_on = drop_duplicates_on)
                if existing.equals(new_slice):
                    # If 'new' data has actually not lead to any modification, skip writing step.
                    continue
            elif new_slice.empty:
                # Do nothing
                continue
            # If `existing` is empty, `new_slice` has not been modified and is written 'as is'.
            # Time to write.
            if dir_name[-1] == '/':
                fname = dir_name + 'part.' + str(int(start.timestamp())) + '.parquet' 
            else:
                fname = dir_name + '/part.' + str(int(start.timestamp())) + '.parquet' 
            fastparquet.write(fname, new_slice)
    else:
        # No data already existing.
        if not path.exists(dir_name):
            mkdir(dir_name)
        # Simple write
        for offset in offset_list:
            start = offset.start_time
            end = offset.end_time
            new_slice = data.loc[(data.index >= start) & (data.index <= end)]
            if dir_name[-1] == '/':
                fname = dir_name + 'part.' + str(int(start.timestamp())) + '.parquet' 
            else:
                fname = dir_name + '/part.' + str(int(start.timestamp())) + '.parquet' 
            fastparquet.write(fname, new_slice)
    # Re-create metadata
    # /!\ Ideally, part to be optimized: only deal with metadata having been actually modified.
    # Sorting as `os.scandir` yield files in arbitrary order.
    file_list = sorted([f.path for f in scandir(dir_name) if f.name[-8:] == '.parquet'])
    merge(file_list)        
