import schedule
import time
import subprocess

schedule_time = "23:48"


"""Method to run scheduled sph youtube data scripts"""


def run_sph_yt_data_scripts():
    print("Starting fetch_youtube_data.py...")
    try:
        result_data = subprocess.run(["python3", "../raw-data-ingestion/batch/fetch_youtube_data.py"], check=True)
        print("fetch_youtube_data.py completed successfully.")
        if result_data.returncode == 0:
            print("Running process_youtube_data.py...")
            process_data = subprocess.run(["python3", "../etl/jobs/process_youtube_data.py"], check=True)
            if process_data.returncode == 0:
                print("Running create_tables.py...")
                subprocess.run(["python3", "../athena/create_tables.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running schedule: {e}")


print(f"Scheduling SPH Youtube data script at {schedule_time}...")
schedule.every().day.at(schedule_time).do(run_sph_yt_data_scripts)

while True:
    schedule.run_pending()
    time.sleep(1)
