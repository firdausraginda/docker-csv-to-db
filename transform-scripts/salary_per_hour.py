import pathlib
import polars as pl
import calendar


data_source_config_dict = {
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

def get_path_src_files(folder_name="src"):
    current_path = pathlib.Path(__file__).absolute()
    join_path_to_src = current_path.parent.parent.joinpath(folder_name)
    return join_path_to_src

def df_from_csv(path_to_src, csv_file_name):
    return pl.read_csv(
        f"{path_to_src}/{csv_file_name}",
        schema=data_source_config_dict[csv_file_name.replace(".csv", "")]
    )

if __name__ == "__main__":
    employee_csv_name = "employees.csv"
    timesheets_csv_name = "timesheets.csv"
    path_to_src = get_path_src_files()

    df_employees = df_from_csv(path_to_src, employee_csv_name)
    df_timesheets = df_from_csv(path_to_src, timesheets_csv_name)

    df_timesheets_cast_checkin_checkout = df_timesheets.with_columns([
        pl.col("checkout").str.to_time("%H:%M:%S"),
        pl.col("checkin").str.to_time("%H:%M:%S")
    ])

    # exclude missing checkin or checkout
    df_timesheets_exclude_missing_checkin_checkout = df_timesheets_cast_checkin_checkout.filter(
        pl.col("checkin").is_not_null() & pl.col("checkout").is_not_null()
    )

    # remove duplication per employee_id, date, checkin, checkout
    df_eligible_timesheets = df_timesheets_exclude_missing_checkin_checkout.group_by(
        ["employee_id", "date", "checkin", "checkout"]).agg(pl.count("*"))

    # handle duplicate employee by calculating the avg salary
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

    df_employees_not_in_duplicate = df_employees.filter(
        ~pl.col("employee_id").is_in(df_duplicate_employees.select("employee_id"))
    ).select(["employee_id", "branch_id", "salary", "join_date", "resign_date"])

    # join non-duplicate employee with duplicated employee using avg salary
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

    df_total_hour_per_monthyear_branch_employee = df_total_hour_per_monthyear_branch_employee.with_columns(
        pl.col("month").map_elements(lambda month_num: calendar.month_name[int(month_num)])
    )

    df_divide_total_summary_with_working_hour = df_total_hour_per_monthyear_branch_employee.group_by(
        ["year", "month", "branch_id"]
    ).agg(pl.sum("salary") / pl.sum("total_working_hour")).select(
        pl.col("year"), pl.col("month"), pl.col("branch_id"), pl.col("salary").round(2).alias("salary_per_hour")
    )

    print(df_divide_total_summary_with_working_hour)
