from os import path, scandir, mkdir
import pandas as pd
import fastparquet
from fastparquet.writer import merge

class WriteException(Exception):
    def __init__(self, message):
        self.message = message

def _previous_date_offset(time_marker, offset: str) -> pd.Timestamp:
    """

    Parameters
      * time_marker (pd.Timestamp or pd.Period)
          Time marker from which is assessed closest earlier on offset timestamp.
      * offset (str)
          Acceptable freq string according pandas library, see https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects

    Returns
      * pd.Timestamp
          Closest earlier timestamp on offset considering midnight anchoring from input time marker.

    """

    if isinstance(time_marker, pd.Period):
        # Case `time_marker` is a period.
        time_marker = time_marker.start_time
    # Assess offset duration
    offset_period = pd.Period(time_marker, freq = offset) 
    start_time = offset_period.start_time
    end_time = offset_period.end_time
    if end_time - start_time < pd.Timedelta('1D'):
        # Transform to pd.DateOffset
        offset = pd.tseries.frequencies.to_offset(offset)
        midnight = start_time.normalize()
        n=(start_time-midnight)//offset
        return midnight + n*offset
    else:
        return start_time

def write(dir_name: str, data: pd.DataFrame, append: bool = True,
          date_group_offset: str = None, group_on : str = None,
          drop_duplicates_on: str = None):
    """

    Implementation to merge data to an existing parquet Dataset, using Datetime partitioning.

    Features and limitations:
      * one row group per parquet file.
      * index does not need to be sorted. It will be sorted in this function.
      * feature: partitions are limited by time period (`pandas.Period`)
      * feature: all `pandas.Period` (i.e. partitions) are anchored to midnight
      * feature: parquet file names embed period start time (rounded to seconds).
        This allows user to merge data that is actually older than already recorded data.
      * feature: 2 different merging scenarii are possible
          * append 'new' data that can be older or newer in time versus existing data.
          * correct existing data.
      * feature: user can specify by `group_on` the name of the column embedding date data to be used for date grouping.
      * limitation: once `date_group_offset` is defined for the 1st write, it should be always the same at each new data merging.
      * limitation: `date_group_offset` only work with DataFrame having a datetime or period data, that can then be turned into `pandas.DatetimeIndex` or `pandas.PeriodIndex`
      * limitation: `date_group_offset` should not be lower than a second (because parquet filenames are rounded to the second)

    Parameters
      * dir_name (str)
          Output directory name.
      * data (pd.DataFrame)
          Data to append to an existing Parquet dataset.
      * date_group_offset (str, optional)
          Date offset to be used to group rows / parquet files (one row group per parquet file).
          The default is None. Provided date group offset is automatically anchored to midnight.
          Because of midnight anchoring,
           * if a set of hours or minutes or seconds , it has to divide 24 hours in complete hours/minutes/seconds, so that 2 consecutive days split the same way.
           * if counted in day, week, month or year, it has to be a single unit (because no anchoring is provided for these larger timeframes)
           Examples of valid values: '2H', 'D', 'W', 'M'
           Examples of invalid values: '5H', '2D', '2W'...
      * group_on (str, optional)
          Name of the column to use for grouping by date. If `None` index is used. Default to `None`.
      * append (bool, optional)
          Required to be set on `True` to update existing parquet files when partitioning is based on `date_group_offset`.
          The default is True.
      * drop_duplicates_on (str or list, optional)
          Either `None`, or a list specifying names of the columns to be used to identify duplicates.
            * if a list, date-like data is necessarily added to this list. If can be only the name of the column/index embedding the date-like data.
            * if `None`, both date-like data and all columns are used to identify duplicates.
          Dropping duplicates on column values without the date-like data used for partitioning is not within the scope of the 'append' mode.
          The main reason is that duplicates are only searched within the same partition, that is defined according the date-like data.
          Would two rows be identified as duplicates only by use of their column values, they could be on two different partitions (not same date).
          For this latter case, the 'overwrite' mode is proposed for implementation.


    ToDo.
      * how storing `date_group_offset` and `group_on` information in metadata, so that it is re-used when merging new data? 
        (user should not have to specify this information at each new merging, as once set, this data has not to be modified)

    Further improvement that could be considered.
      * metadata is re-created from scratch. This should/could be optimized?
      * change 'append' parameter into 'write_mode' with values `None`, 'append' or 'overwrite'
          * with 'append', user can only add new data or modify existing data, keeping at least the same Datetime-based index -> mode currently implemented here.
          * with `overwrite`, the input data fully replaces data of the partition(s) it overlaps (can be used to remove data from existing partitions). This would require
            the user to delimit the start/end of the data by fully empty row (if start/end of new data do not match exactly the slice to overwrite)
      * (not willing to delve into that) allow partitioning on float data instead of necessarily DateTimeIndex? (some complexity: offset definition with anchoring is to be defined)

    (side notes to be removed)
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
           date_group_offset = str or pd.DateOffset
           group_on = str
           partition_on = [] kept as is
           append: True / or write_mode = 'append'

    """

    # Sort & store index in generic variable.
    if group_on:
        # Case user specifies a column to be used for date grouping.
        data = data.sort_values(group_on)
        date_index = pd.Index(data[group_on])
    else:
        # Case DataFrame index is to be used.
        data = data.sort_index()
        date_index = data.index

    # Check `date_index` is a DatetimeIndex or PeriodIndex
    if not (isinstance(date_index, pd.DatetimeIndex) or isinstance(date_index, pd.PeriodIndex)):
        raise WriteException('Specified grouping data stored in {!s} does not appear to be a date-like or period-like data.'.format(date_index.name))
    
    # 1st draft implementation, not using any _metadata, first to explain the idea.
    # /!\ data retrieval to be implemented by reading from _metadata /!\
    # If appending, check existing target parquet dataset has
    #  - same `date_group_offset`, e.g '2H'...
    #  - if `group_on` used, same column 'name' embedding the date data to be used for grouping
    # Can it be store/read from metadata?
    # When appending, does not need to be provided again by the user: a single ìndex_offsets` is imposed.

    # Generate list of timestamps on offset to split the data.
    start_time = _previous_date_offset(date_index[0], date_group_offset)

    if isinstance(date_index, pd.PeriodIndex):
        # Case data is a `PeriodIndex`.
        date_index = date_index.start_time

    offset_list = pd.period_range(start = start_time, end = date_index[-1], freq = date_group_offset)

    # If appending, and some parquet files already exist.
    if append and path.exists(dir_name) and [f for f in scandir(dir_name) if f.name[-8:] == '.parquet'] != []:
        fptf = fastparquet.ParquetFile(dir_name)
        if group_on:
            # Date-like data to be used is either provided by `group_on` or is the DataFrame index, which is then in 1st position.
            date_index_name = group_on
        else:
            date_index_name = fptf.columns[0]
            # Move the date-like data as a column.
            data = data.reset_index()

        # Formatting `drop_duplicates_on` for use in `pd.concat()`.
        if drop_duplicates_on:
            if isinstance(drop_duplicates_on, str):
                # Case `drop_duplicates_on` is a single column name.
                drop_duplicates_on = [drop_duplicates_on]
            if not date_index_name in drop_duplicates_on:
                drop_duplicates_on.append(date_index_name)
            # Check all values in `drop_duplicates_on` exist in column list.
            if not all(item in fptf.columns for item in drop_duplicates_on):
                raise WriteException('At least one value in {!s} is not a column/index name as in {!s}.'.format(str(drop_duplicates_on), str(list(fptf.columns))))

        # Setup iteration over offset list
        for offset in offset_list:
            # Load existing data if any
            start = offset.start_time
            end = offset.end_time
            existing = fptf.to_pandas(filters=[(date_index_name, '>=', start), (date_index_name, '<=', end)])           
            new_slice = data.loc[(date_index >= start) & (date_index <= end)]
            if not (existing.empty or new_slice.empty):
                # Both DataFrames exist, merge them.
                if not group_on:
                    # If date-like data is the index, reset it for merging in legacy data as well.
                    existing = existing.reset_index()
                new_slice = pd.concat([existing, new_slice]).drop_duplicates(subset = drop_duplicates_on, keep='last').sort_values(date_index_name)
                if existing.equals(new_slice):
                    # If no new DataFrame resulting from the merge, skip writing step.
                    continue
            elif new_slice.empty:
                # Do nothing
                continue
            # If `existing` is empty, `new_slice` has not been modified and it is then written 'as is'.
            # If date-like data was in index, restore it as index.
            if not group_on:
                new_slice = new_slice.set_index(date_index_name)
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
            new_slice = data.loc[(date_index >= start) & (date_index <= end)]
            if new_slice.empty:
                continue
            else:
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
