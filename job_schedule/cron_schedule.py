import schedule
import time
import subprocess

schedule_time = "21:31"


"""Method to run scheduled sph youtube data scripts"""


def run_sph_yt_data_scripts():
    print("Starting fetch_youtube_data.py...")
    try:
        result = subprocess.run(["python3", "../raw-data-ingestion/batch/fetch_youtube_data.py"], check=True)
        print("fetch_youtube_data.py completed successfully.")
        if result.returncode == 0:
            print("Running process_youtube_sph_data.py...")
            subprocess.run(["python3", "../etl/jobs/process_youtube_sph_data.py"])
    except Exception as e:
        print(f"Error running schedule: {e}")


print(f"Scheduling SPH Youtube data script at {schedule_time}...")
schedule.every().day.at(schedule_time).do(run_sph_yt_data_scripts)

while True:
    schedule.run_pending()
    time.sleep(1)
