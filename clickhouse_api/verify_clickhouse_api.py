import os
import sys

# Add project root to python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from app import create_app

# Set environment variables for testing
os.environ['USE_CLICKHOUSE'] = 'true'
os.environ['CH_HOST'] = 'localhost'
os.environ['CH_PORT'] = '8123'
os.environ['CH_USER'] = 'default'
os.environ['CH_PASSWORD'] = ''
os.environ['CH_DATABASE'] = 'rag'

app = create_app()
client = app.test_client()

def test_works_endpoint():
    print("Testing /works endpoint...")
    try:
        response = client.get('/works?per_page=1')
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            data = response.get_json()
            print(f"Results count: {len(data.get('results', []))}")
            if data.get('results'):
                print(f"First result title: {data['results'][0].get('display_name')}")
        else:
            print(f"Error: {response.data}")
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    test_works_endpoint()
