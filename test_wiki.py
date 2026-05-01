import sseclient
import requests
import json

print("Connecting to Wikipedia stream...")
url = 'https://stream.wikimedia.org/v2/stream/recentchange'
headers = {
    'User-Agent': 'TweetPulse/1.0 (Learning project; nitantvaidya@outlook.com)'
}
r = requests.get(url, stream=True, headers=headers)
print(f"Status code: {r.status_code}")
c = sseclient.SSEClient(r)
print("Connected! Waiting for events...")
count = 0
for e in c.events():
    if e.data:
        try:
            d = json.loads(e.data)
            if d.get('type') == 'edit' and d.get('wiki') == 'enwiki':
                print(d['title'], '|', d['user'])
                count += 1
                if count >= 5:
                    break
        except:
            pass
print(f"Done! Got {count} events")