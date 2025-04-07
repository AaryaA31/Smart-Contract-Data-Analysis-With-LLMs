import requests
import pandas as pd

url = "https://api.studio.thegraph.com/query/108677/seaport-subgraph/v0.0.1"

query = """
{
  orderFulfilleds(first: 50, orderBy: blockTimestamp, orderDirection: desc) {
    id
    orderHash
    offerer
    recipient
    zone
    blockTimestamp
    transactionHash
  }
}
"""

response = requests.post(url, json={"query": query})
response_json = response.json()
print("Raw response:")
print(response_json)

data = response_json.get("data", {}).get("orderFulfilleds", [])

if data:
    df = pd.DataFrame(data)
    df.to_csv("seaport_order_fulfilled.csv", index=False)
    print(f"✅ CSV saved with {len(df)} rows.")
else:
    print("⚠️ No data returned from the subgraph.")
