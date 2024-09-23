import os
import datetime
import pandas as pd


def aggregate_files():
    actual_date = datetime.datetime.now() + datetime.timedelta(hours=3)
    output_directory = "/opt/airflow/dags/output"
    files = os.listdir("/opt/airflow/dags/input")
    data_array = []
    if isinstance(actual_date, str):
        actual_date = datetime.datetime.strptime(actual_date, '%Y-%m-%d')
    for file in files:
        file_date = file.split(".")[0]
        date_diff = (actual_date - datetime.datetime.strptime(file_date, '%Y-%m-%d')).days
        if 1 <= date_diff <= 7:
            logs_data = pd.read_csv(f"/opt/airflow/dags/input/{file}", header=None)
            logs_data.columns = ["email", "action", "date"]
            logs_data = logs_data.drop('date', axis=1)
            data_array.append(logs_data)
    # если для подходящей даты логов нет - сохраним в csv следующий, "пустой" результат:
    if len(data_array) == 0:
        empty_data = pd.DataFrame({"email": [None], "create_count": [0],
                                   "read_count": [0], "update_count": [0], "delete_count": [0]})

        empty_data.to_csv(f"/opt/airflow/dags/output/{actual_date.date()}.csv", index=False)
    else:
        summary_logs = pd.concat(data_array)
        aggregate_logs = (summary_logs.groupby(['email', 'action'])['action']
                          .count()
                          .unstack(fill_value=0)
                          .reset_index())
        aggregate_logs = aggregate_logs[["email", "CREATE", "READ", "UPDATE", "DELETE"]]
        aggregate_logs.columns = ["email", "create_count", "read_count", "update_count", "delete_count"]
        aggregate_logs.to_csv(f"{output_directory}/{actual_date.date()}.csv", index=False)
