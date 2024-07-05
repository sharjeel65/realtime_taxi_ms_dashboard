import time
from geopy.distance import geodesic


# Function to calculate the speed
def calculate_speed(coord1, coord2, time_interval):
    """
    Calculate the speed given two coordinates and the time interval.

    Parameters:
    coord1 (tuple): (latitude, longitude) of the first coordinate.
    coord2 (tuple): (latitude, longitude) of the second coordinate.
    time_interval (float): Time interval in seconds.

    Returns:
    float: Speed in km/h.
    """
    # Calculate the distance between the two coordinates in kilometers
    distance = geodesic(coord1, coord2).kilometers

    # Calculate speed (distance / time)
    speed = (distance / time_interval) * 3600  # convert to km/h
    return speed


# Sample data: Replace this with real-time data acquisition
coordinates = [
    (40.712776, -74.005974),  # Example coordinates (latitude, longitude)
    (40.713776, -74.004974),
    (40.714776, -74.003974),
    # Add more coordinates as needed
]

time_interval = 5  # Time interval in seconds

# Simulate real-time data update every 5 seconds
for i in range(1, len(coordinates)):
    coord1 = coordinates[i - 1]
    coord2 = coordinates[i]
    speed = calculate_speed(coord1, coord2, time_interval)
    print(f"Speed between point {i - 1} and point {i}: {speed:.2f} km/h")

    # Wait for the next update (5 seconds)
    time.sleep(time_interval)