import requests
import pandas as pd
from datetime import datetime

url = "https://api.studio.thegraph.com/query/108677/seaport-subgraph/v0.0.1"
query = """
{
  orderFulfilleds(first: 1000, orderBy: blockTimestamp, orderDirection: desc) {
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


data = response_json.get("data", {}).get("orderFulfilleds", [])

if data:
    df = pd.DataFrame(data)


    df["blockTimestamp"] = pd.to_datetime(df["blockTimestamp"], unit="s")
    df["date"] = df["blockTimestamp"].dt.date
    df["hour"] = df["blockTimestamp"].dt.hour

    df = df.sort_values("blockTimestamp").reset_index(drop=True)


    df.rename(columns={
        "id": "Event ID",
        "orderHash": "Order Hash",
        "offerer": "Offerer Address",
        "recipient": "Recipient Address",
        "zone": "Zone Address",
        "blockTimestamp": "Timestamp",
        "transactionHash": "Tx Hash"
    }, inplace=True)


    daily_counts = df.groupby("date").size().reset_index(name="Orders Per Day")
    df = df.merge(daily_counts, on="date", how="left")


    tx_counts = df["Tx Hash"].value_counts().rename("Tx Count")
    df = df.merge(tx_counts, left_on="Tx Hash", right_index=True)
    df["Trade Type"] = df["Tx Count"].apply(lambda x: "Batch Trade" if x > 1 else "Individual Trade")

    filename = "seaport_order_fulfilled_with_trade_type.csv"
    df.to_csv(filename, index=False)

    print(f"✅ CSV saved as '{filename}' with trade classification included.")
else:
    print("⚠️ No data returned from the subgraph.")
