import pathlib
import os
import polars as pl

data_source_config_dict = {
    "employees": {
            "employe_id": pl.String,
            "branch_id": pl.String,
            "salary": pl.Int64,
            "join_date": pl.Date,
            "resign_date": pl.Date
        }
}

def get_path_src_files():
    current_path = pathlib.Path(__file__).absolute()
    join_path_to_src = current_path.parent.joinpath("src")
    return join_path_to_src

path_to_src = get_path_src_files()
src_files = os.listdir(path_to_src)

for src_file in src_files:
    if src_file == 'employees.csv':
        df = pl.read_csv(
            f"{path_to_src}/{src_file}",
            schema=data_source_config_dict[src_file.replace(".csv", "")]
        )
        print(df)
