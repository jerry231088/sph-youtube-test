import schedule
import time
import subprocess


def run_test_script():
    print("Starting test.py...")
    try:
        result = subprocess.run(["python3", "/path/to/test.py"], check=True)
        print("test.py completed successfully.")
        if result.returncode == 0:
            print("Running next_script.py...")
            subprocess.run(["python3", "/path/to/next_script.py"])
    except Exception as e:
        print(f"Error running test.py: {e}")

print("Scheduling test.py at 09:00...")
schedule.every().day.at("09:00").do(run_test_script)

while True:
    schedule.run_pending()
    time.sleep(1)
