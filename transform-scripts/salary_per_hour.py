import pathlib
import polars as pl
from dotenv import load_dotenv
import os
import calendar


# define constant variables
DATA_SOURCE_CONFIG_DICT = {
    "employees": {
        "employee_id": pl.Utf8,
        "branch_id": pl.Utf8,
        "salary": pl.Int64,
        "join_date": pl.Date,
        "resign_date": pl.Date
    },
    "timesheets": {
        "timesheet_id": pl.Utf8,
        "employee_id": pl.Utf8,
        "date": pl.Date,
        "checkin": pl.Utf8,
        "checkout": pl.Utf8,
    }
}

DB_CONN_DICT = {
    "PORT": "", 
    "HOST": "", 
    "POSTGRES_DB": "", 
    "POSTGRES_PASSWORD": "", 
    "POSTGRES_USER": ""
}


def df_from_csv(path_to_src, csv_file_name):
    """create dataframe from csv file with given specification"""

    return pl.read_csv(
        f"{path_to_src}/{csv_file_name}",
        schema=DATA_SOURCE_CONFIG_DICT[csv_file_name.replace(".csv", "")]
    )

def get_path_src_dir(folder_name="src"):
    """return absolute path to src directory"""

    current_path = pathlib.Path(__file__).absolute()
    join_path_to_src = current_path.parent.parent.joinpath(folder_name)
    return join_path_to_src

def transform_data(df_employees, df_timesheets):
    """transform employee & timesheets data, and return salary per hour"""

    # convert checkin & checkout from string to time
    df_timesheets_cast_checkin_checkout = df_timesheets.with_columns([
        pl.col("checkout").str.to_time("%H:%M:%S"),
        pl.col("checkin").str.to_time("%H:%M:%S")
    ])

    # exclude missing checkin or checkout from df_timesheets
    df_timesheets_exclude_missing_checkin_checkout = df_timesheets_cast_checkin_checkout.filter(
        pl.col("checkin").is_not_null() & pl.col("checkout").is_not_null()
    )

    # remove df_timesheets duplication per employee_id, date, checkin, checkout
    df_eligible_timesheets = df_timesheets_exclude_missing_checkin_checkout.group_by(
        ["employee_id", "date", "checkin", "checkout"]).agg(pl.count("*"))

    # get duplicate employees only
    # handle duplicate employees by calculate the avg salary
    df_duplicate_employees = df_employees.group_by("employee_id").count().filter(pl.col("count") > 1)
    df_avg_salary_duplicated_employees = df_employees.join(
        df_duplicate_employees, on="employee_id", how="inner"
    ).group_by("employee_id").agg(
        pl.sum("salary") / pl.count("employee_id")
    ).select(pl.col("employee_id"), pl.col("salary").cast(pl.Int64).alias("avg_salary"))
    df_duplicate_employees_with_avg_salary = df_employees.join(df_avg_salary_duplicated_employees, on="employee_id", how="inner").select([
        "employee_id", "branch_id", "avg_salary","join_date", "resign_date"
    ]).group_by(["employee_id", "branch_id", "avg_salary","join_date", "resign_date"]).agg(pl.count("*"))
    df_duplicate_employees_with_avg_salary = df_duplicate_employees_with_avg_salary.rename({"avg_salary": "salary"})

    # get unique employees only
    df_employees_not_in_duplicate = df_employees.filter(
        ~pl.col("employee_id").is_in(df_duplicate_employees.select("employee_id"))
    ).select(["employee_id", "branch_id", "salary", "join_date", "resign_date"])

    # join non-duplicate employees with the duplicated employees
    df_eligible_employees = pl.concat([
        df_employees_not_in_duplicate,
        df_duplicate_employees_with_avg_salary
    ], how="vertical")

    # found checkout time < checkin time. handle by use checkin as checkout, and vice versa
    # calculate `checkout - checkin`, then convert it in hour level
    df_convert_time_to_hour_level = df_eligible_timesheets.with_columns(
        checkin=pl.col("checkin").dt.hour() + pl.col("checkin").dt.minute()/60 + pl.col("checkin").dt.second()/3600,
        checkout=pl.col("checkout").dt.hour() + pl.col("checkout").dt.minute()/60 + pl.col("checkout").dt.second()/3600
    )
    df_calculate_working_hour = df_convert_time_to_hour_level.with_columns(
        working_hour=pl.when(pl.col("checkout") < pl.col("checkin")).then(pl.col("checkin") - pl.col("checkout")).otherwise(pl.col("checkout") - pl.col("checkin"))
    )

    # sum working hours per year, month, branch_id, & employee_id
    df_total_hour_per_monthyear_branch_employee = df_calculate_working_hour.join(
        df_eligible_employees, on="employee_id", how="left"
    ).group_by([
        pl.col("date").dt.year().cast(pl.Utf8).alias("year"),
        pl.col("date").dt.month().cast(pl.Utf8).alias("month"),
        pl.col("branch_id"),
        pl.col("employee_id"),
        pl.col("salary")
        ]).agg(pl.sum("working_hour").alias("total_working_hour"))

    # convert month number to month name
    df_total_hour_per_monthyear_branch_employee = df_total_hour_per_monthyear_branch_employee.with_columns(
        pl.col("month").map_elements(lambda month_num: calendar.month_name[int(month_num)])
    )

    # divide total salary with total working_hour 
    df_divide_total_summary_with_working_hour = df_total_hour_per_monthyear_branch_employee.group_by(
        ["year", "month", "branch_id"]
    ).agg(pl.sum("salary") / pl.sum("total_working_hour")).select(
        pl.col("year"), pl.col("month"), pl.col("branch_id"), pl.col("salary").round(2).alias("salary_per_hour")
    )

    return df_divide_total_summary_with_working_hour

def get_db_connection_uri():
    """return connection uri to DB with config from .env file"""

    load_dotenv()
    [DB_CONN_DICT.update({key:os.environ[key]}) for key in DB_CONN_DICT.keys()]
    conn = f"postgresql://{DB_CONN_DICT["POSTGRES_USER"]}:{DB_CONN_DICT["POSTGRES_PASSWORD"]}@{DB_CONN_DICT["HOST"]}:{DB_CONN_DICT["PORT"]}/{DB_CONN_DICT["POSTGRES_DB"]}"
    return conn

def insert_to_db(df, conn_uri, dest_table_name):
    """insert dataframe to DB"""

    try:
        df.write_database(
            dest_table_name,
            connection=conn_uri,
            if_table_exists="append",
            engine="adbc"
        )
        print(f"successfully append data to table '{dest_table_name}' !")
    except Exception as e:
        if e.sqlstate == "42P01":
            df.write_database(
                dest_table_name,
                connection=conn_uri,
                if_table_exists="replace",
                engine="adbc"
            )
            print(f"table '{dest_table_name}' is not exists, will create it for the first time ...")
            print(f"successfully create table '{dest_table_name}', and write the data !")
        else:
            raise e

def read_data_from_db(query, conn_uri):
    """execute query to select data from DB"""
    
    try:
        df_result = pl.read_database_uri(
            query=query,
            uri=conn_uri,
            engine="adbc"
        )
    except Exception as e:
        raise e
    else:
        return df_result


if __name__ == "__main__":
    # define csv name
    employee_csv_name = "employees.csv"
    timesheets_csv_name = "timesheets.csv"

    # create dataframe
    path_to_src = get_path_src_dir()
    df_employees = df_from_csv(path_to_src, employee_csv_name)
    df_timesheets = df_from_csv(path_to_src, timesheets_csv_name)

    df_salary_per_hour = transform_data(df_employees, df_timesheets)

    conn_uri = get_db_connection_uri()

    dest_table_name = "salary_per_hour"
    insert_to_db(df_salary_per_hour, conn_uri, dest_table_name)

    # query = """
    #     SELECT *
    #     FROM salary_per_hour
    #     LIMIT 100
    # """
    # df_read_salary_per_hour = read_data_from_db(query, conn_uri)
    # print(df_read_salary_per_hour.head())