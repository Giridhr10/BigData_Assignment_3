import json
import requests
import redis
import matplotlib.pyplot as plt

class RandomUserProcessor: # This class to process data from the Random User Generator API and insert into RedisJSON
    
    def __init__(self, redis_host='redis-17238.c251.east-us-mz.azure.cloud.redislabs.com', redis_port=17238,password= 'eOzVqYVreGmm6QWQT0hYx2KhJHElYPAL'):
        
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, password= password)

    def fetch_random_users(self, num_users=10):
        # Fetch random user data from the Random User Generator API.

        url = f'https://randomuser.me/api/?results={num_users}'
        response = requests.get(url)
        data = response.json()
        return data['results']

    def insert_into_redis(self, user_data):
      # Insert user data into RedisJSON.
        for index, user in enumerate(user_data):
            redis_key = f'user:{index}'
            self.redis_client.set(redis_key, json.dumps(user))

    def generate_age_histogram(self):
        # histogram of ages of users and display using matplotlib.
        ages = []
        for key in self.redis_client.scan_iter(match='user:*'):
            user_data = json.loads(self.redis_client.get(key))
            age = user_data['dob']['age']
            ages.append(age)

        plt.hist(ages, bins=10, edgecolor='black')
        plt.xlabel('Age')
        plt.ylabel('Frequency')
        plt.title('Age Distribution of Users')
        plt.show()

    def perform_aggregation(self): # aggregation on user data.
       
        total_users = self.redis_client.dbsize()
        print(f"Total number of users: {total_users}")

    def print_user_data(self): # Print all user data stored in Redis.

        for key in self.redis_client.scan_iter(match='user:*'):
            user_data = json.loads(self.redis_client.get(key))
            print(user_data)

    def search_user(self): # Search for a user based on query parameters entered by the user.

        while True:
            query = input("Enter query to search (Enter 'x' to exit): ")
            if query.lower() == 'x':
                break
            found_users = []
            for key in self.redis_client.scan_iter(match='user:*'):
                user_data = json.loads(self.redis_client.get(key))
                for key, value in user_data.items():
                    if query.lower() in str(value).lower():
                        found_users.append(user_data)
                        break
            if found_users:
                print("Found users:")
                for user in found_users:
                    print(user)
            else:
                print("No users found.")

    def run(self):
        # Fetch random user data
        user_data = self.fetch_random_users()

        # Insert into RedisJSON
        self.insert_into_redis(user_data)

        # Generate age histogram
        self.generate_age_histogram()

        # Perform aggregation
        self.perform_aggregation()

        # Print user data
        self.print_user_data()

        # Search for users
        self.search_user()

if __name__ == "__main__":
    processor = RandomUserProcessor()
    processor.run()