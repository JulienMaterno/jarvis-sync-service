import httpx
import os
import json
from dotenv import load_dotenv

load_dotenv()

url = "https://jarvis-intelligence-service-776871804948.asia-southeast1.run.app/api/v1/journal/evening-prompt"

# Mock activity data for testing
payload = {
    "activity_data": {
        "meetings": [
            {
                "title": "Strategy Sync with David",
                "summary": "Discussed Q1 roadmap and hiring plans. Need to finalize budget by Friday.",
                "people_mentioned": ["David", "Sarah"]
            }
        ],
        "calendar_events": [
            {"summary": "Lunch with Maria"},
            {"summary": "Team Standup"}
        ],
        "emails": [
            {"subject": "Project Update", "sender": "john@example.com", "contact_name": "John Doe"}
        ],
        "tasks_completed": [
            {"title": "Submit expense report"},
            {"title": "Review PR #123"}
        ],
        "tasks_created": [
            {"title": "Schedule follow-up with client"}
        ],
        "reflections": [],
        "journals": []
    }
}

print(f"Testing evening journal prompt at {url}...")
try:
    response = httpx.post(url, json=payload, timeout=60.0)
    print(f"Status Code: {response.status_code}")
    if response.status_code == 200:
        print("Response:")
        print(json.dumps(response.json(), indent=2))
    else:
        print(f"Error: {response.text}")
except Exception as e:
    print(f"Request failed: {e}")
