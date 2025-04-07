import requests
import pandas as pd

url = "https://api.studio.thegraph.com/query/108677/seaport-subgraph/v0.0.1"

# Parameters for pagination
all_data = []
skip = 0
batch_size = 1000
max_batches = 5  # Adjust this for more data (max_batches * batch_size)

for i in range(max_batches):
    query = f"""
    {{
      orderFulfilleds(first: {batch_size}, skip: {skip}, orderBy: blockTimestamp, orderDirection: desc) {{
        id
        orderHash
        offerer
        recipient
        zone
        blockTimestamp
        transactionHash
      }}
    }}
    """

    response = requests.post(url, json={"query": query})
    response_json = response.json()

    batch_data = response_json.get("data", {}).get("orderFulfilleds", [])

    if not batch_data:
        print("No more data to fetch.")
        break

    all_data.extend(batch_data)
    skip += batch_size
    print(f"Fetched batch {i + 1} with {len(batch_data)} records.")

# Convert collected data to DataFrame
df = pd.DataFrame(all_data)

# Data processing
df["blockTimestamp"] = pd.to_datetime(df["blockTimestamp"], unit="s")
df["date"] = df["blockTimestamp"].dt.date
df["hour"] = df["blockTimestamp"].dt.hour
df = df.sort_values("blockTimestamp").reset_index(drop=True)

# Rename columns for clarity
df.rename(columns={
    "id": "Event ID",
    "orderHash": "Order Hash",
    "offerer": "Offerer Address",
    "recipient": "Recipient Address",
    "zone": "Zone Address",
    "blockTimestamp": "Timestamp",
    "transactionHash": "Tx Hash"
}, inplace=True)

# Calculate daily counts
daily_counts = df.groupby("date").size().reset_index(name="Orders Per Day")
df = df.merge(daily_counts, on="date", how="left")

# Classify trade types
tx_counts = df["Tx Hash"].value_counts().rename("Tx Count")
df = df.merge(tx_counts, left_on="Tx Hash", right_index=True)
df["Trade Type"] = df["Tx Count"].apply(lambda x: "Batch Trade" if x > 1 else "Individual Trade")

# Save to CSV
filename = "seaport_order_fulfilled_with_trade_type.csv"
df.to_csv(filename, index=False)

print(f"✅ CSV saved as '{filename}' with {len(df)} total records.")