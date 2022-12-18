## Performance Comparison between join and merge with different indices
# Performs a performance comparison with the following steps:
#   1. Create a number of data-frame-tuples to join with each other
#   2. Execute a number of different combinations of join and merges with and without indices and gather the result (execution time).

## Room for improvement:
# - Aggregate the results
# - restructure the table with the results according to the following schema:
#       Method(Join/Merge) | RandomNumberRange | Number of Rows | IndexLeftSet | IndexRightSet | Execution Time |Dataset-Number(ID)
# - vary the number of lines and the range of random numbers and analyze their impact on the performance
# - compare the different joins: 'Left', 'Right', 'Inner', 'Outer', 'Full' (currently only 'Left' is used)
# - parallelize the execution of the benchmark

#%%
import pandas as pd
import numpy as np
import time
import pickle
import tempfile
import shutil


# Create a tuple of two Benchmark Dataframes to join later in the benchmark
def create_benchmark_dataframe_tuple_to_join(
    number_of_rows: int,
    number_of_columns: int,
    random_number_min: int,
    random_number_max: int,
):
    df1 = pd.DataFrame(
        np.random.randint(
            random_number_min,
            random_number_max,
            size=(number_of_rows, number_of_columns),
        ),
        columns=list("ABC"),
    )
    df2 = pd.DataFrame(
        np.random.randint(
            random_number_min,
            random_number_max,
            size=(number_of_rows, number_of_columns),
        ),
        columns=list("CDE"),
    )
    return df1, df2


# Create a number of dataframe-tuples to join later
def create_benchmark_dataframes(number_of_sets: int):
    data = []
    for i in range(0, number_of_sets):
        dfs = create_benchmark_dataframe_tuple_to_join()
        data.append(dfs)
    return data


# Create and store the dataframes as files to prevent memory errors
def create_stored_benchmark_dataframes(
    tempdir,
    number_of_benchmark_sets: int,
    number_of_rows: int,
    number_of_columns: int,
    random_number_min: int,
    random_number_max: int,
):
    print(
        "Create "
        + str(number_of_benchmark_data_sets)
        + " datasets (tuples) with "
        + str(number_of_rows)
        + " rows  and "
        + str(number_of_columns)
        + " columns each."
        + " Random number min: "
        + str(random_number_min)
        + " Random number max: "
        + str(random_number_max)
    )
    filenames_temp = []
    for i in range(0, number_of_benchmark_sets):
        dfs = create_benchmark_dataframe_tuple_to_join(
            number_of_rows,
            number_of_columns,
            random_number_min,
            random_number_max=random_number_max,
        )
        s = pickle.dumps(dfs)
        filename_temp = tempfile.mktemp(dir=tempdir)
        with open(filename_temp, "wb") as file_out:
            file_out.write(s)
        filenames_temp.append(filename_temp)
    return filenames_temp


# Execute one Benchmark for the 'join'-Operation with the specified parameters
def benchmark_join(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    set_index_left: bool,
    set_index_right: bool,
    how="left",
):
    start_time = time.time()
    if set_index_left:
        df1.set_index("C", inplace=True)
    if set_index_right:
        df2.set_index("C", inplace=True)
    result = df1.join(df2, on="C", how=how)
    end_time = time.time()
    return end_time - start_time


# Execute a series of Benchmarks for the 'join'-Operation with the specified parameters
def benchmark_join_aggregate(
    temp_file_names, set_index_left: bool, set_index_right: bool, how="left"
):
    print(
        "benchmark_join_aggregate for "
        + str(len(temp_file_names))
        + " Datasets. index-left: "
        + str(set_index_left)
        + " index-right: "
        + str(set_index_right)
    )
    results = []
    agg_time_start = time.time()
    for filename in temp_file_names:
        df1 = None
        df2 = None
        with open(filename, "rb") as file_in:
            df1, df2 = pickle.loads(file_in.read())
        result = benchmark_join(df1, df2, set_index_left, set_index_right, how=how)
        results.append(result)
    agg_time_end = time.time()
    avg = sum(results) / len(results)
    print("Avg: " + str(avg))
    print("Total: " + str(agg_time_end - agg_time_start))
    return results


# Execute a series of Benchmarks for the 'merge'-Operation with the specified parameters
def benchmark_merge_aggregate(
    temp_file_names, set_index_left: bool, set_index_right: bool
):
    print(
        "benchmark_merge_aggregate for "
        + str(len(temp_file_names))
        + " Datasets. index-left: "
        + str(set_index_left)
        + " index-right: "
        + str(set_index_right)
    )
    results = []
    agg_time_start = time.time()
    for filename in temp_file_names:
        df1 = None
        df2 = None
        with open(filename, "rb") as file_in:
            df1, df2 = pickle.loads(file_in.read())
        result = benchmark_merge(df1, df2, set_index_left, set_index_right)
        results.append(result)
    agg_time_end = time.time()
    avg = sum(results) / len(results)
    print("Avg: " + str(avg))
    print("Total: " + str(agg_time_end - agg_time_start))
    return results


# Execute one Benchmark for the 'merge'-Operation with the specified parameters
def benchmark_merge(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    set_index_left: bool,
    set_index_right: bool,
    how="left",
):
    start_time = time.time()
    merge_parameters = {"how": how, "suffixes": ("df1_", "df2_")}
    merge_parameters["left"] = df1
    if set_index_left:
        df1.set_index("C", inplace=True)
        merge_parameters["left_index"] = True
    else:
        merge_parameters["left_on"] = "C"
    if set_index_right:
        df2.set_index("C", inplace=True)
        merge_parameters["right_index"] = True
    else:
        merge_parameters["right_on"] = "C"
    merge_parameters["left"] = df1
    merge_parameters["right"] = df2
    result = pd.merge(**merge_parameters)
    end_time = time.time()
    return end_time - start_time


#%%
if __name__ == "__main__":
    print("StartBenchmark")

    tempdir = tempfile.mkdtemp(dir=".//data/")

    # constants
    number_of_benchmark_data_sets = 5000
    number_of_rows = 10000
    number_of_columns = 3
    random_number_min = 0
    random_number_max = 100

    # Create the dataframes to use for the benchmark
    # The dataframes are stored and fetched from storage to ensure the comparability between measurements due to using the exact same data.
    tempfile_names = create_stored_benchmark_dataframes(
        tempdir=tempdir,
        number_of_benchmark_sets=number_of_benchmark_data_sets,
        number_of_rows=number_of_rows,
        number_of_columns=number_of_columns,
        random_number_min=random_number_min,
        random_number_max=random_number_max,
    )

    #%% Execute Benchmark
    result_df = pd.DataFrame()
    result_df["JoinIndexLeft"] = benchmark_join_aggregate(tempfile_names, True, False)
    result_df["JoinIndexRight"] = benchmark_join_aggregate(tempfile_names, False, True)
    result_df["JoinIndexLeftRight"] = benchmark_join_aggregate(
        tempfile_names, True, True
    )
    result_df["MergeNoIndex"] = benchmark_merge_aggregate(tempfile_names, False, False)
    result_df["MergeIndexLeft"] = benchmark_merge_aggregate(tempfile_names, True, False)
    result_df["MergeIndexRight"] = benchmark_merge_aggregate(
        tempfile_names, False, True
    )
    result_df["MergeIndexLeftRight"] = benchmark_merge_aggregate(
        tempfile_names, True, True
    )

    # Optional: Use describe() to get an overview over the benchmark data

    #%%
    # Store Results to csv
    result_df.to_csv("result_compare_pandas_join_merge_indices.csv")
    # Delete Temp-Dir
    shutil.rmtree(tempdir)
