import streamlit as st
from minio import Minio
import json
from datetime import datetime

from streamlit_autorefresh import st_autorefresh

client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket = "anomalies-bucket"

def load_events():
    events = []
    try:
        for obj in client.list_objects(bucket, prefix="events/"):
            try:
                response = client.get_object(bucket, obj.object_name)
                for line in response:
                    line = line.decode("utf-8").strip()
                    if line:
                        event = json.loads(line)
                        if "timestamp" in event:
                            try:
                                event["date"] = datetime.fromtimestamp(event["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
                            except Exception:
                                event["date"] = str(event["timestamp"])
                        events.append(event)
            except Exception as e:
                if "NoSuchKey" not in str(e):
                    st.error(f"Erreur MinIO : {e}")
    except Exception as e:
        if "NoSuchKey" not in str(e):
            st.error(f"Erreur MinIO : {e}")
    return events

st.title("Tous les événements e-commerce")

st_autorefresh(interval=5000, key="refresh")

events = load_events()
if events:
    st.dataframe(events)
    achats = [e for e in events if e.get("event_type") == "achat"]
    anomalies = [e for e in achats if e.get("amount", 0) > 400]

    st.metric("Total achats", len(achats))
    st.metric("Total anomalies (achats > 400€)", len(anomalies))

    st.subheader("Anomalies détectées (achats > 400€)")
    if anomalies:
        st.dataframe(anomalies)
    else:
        st.info("Aucune anomalie détectée pour le moment.")
else:
    st.info("Aucun événement détecté pour le moment.")