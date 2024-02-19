import os

print("Fetching environment variables:")

for key, value in os.environ.items():
    print(f"{key}: {value}")