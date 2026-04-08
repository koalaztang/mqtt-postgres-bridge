"""
MQTT to Postgres Bridge

Subscribes to the MQTT topic where the nRF7002-DK publishes energy data,
parses the JSON payload, and inserts rows into a Postgres table.

Using Railway service with these environment variables
referenced from Mosquitto Broker and Postgres services.

Railway Variable References:
  MOSQUITTO_TCP_HOST, MOSQUITTO_TCP_PORT,
  MOSQUITTO_USERNAME, MOSQUITTO_PASSWORD
  DATABASE_URL (from Postgres service)
"""

import os
import json
import time
import psycopg2
import paho.mqtt.client as mqtt

from datetime import datetime, timezone, timedelta

# Config from Railway environment variables
MQTT_HOST = os.environ["MOSQUITTO_TCP_HOST"]
MQTT_PORT = int(os.environ["MOSQUITTO_TCP_PORT"])
MQTT_USER = os.environ["MOSQUITTO_USERNAME"]
MQTT_PASS = os.environ["MOSQUITTO_PASSWORD"]
DATABASE_URL = os.environ["DATABASE_URL"]
MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "capstone/energy")

# Timestamp reconstruction state
# We pair each uptime with the wall-clock time we received it,
# then use uptime differences to compute real timestamps.
last_uptime_ms = 0
last_wall_time = None

"""Connect to Postgres using the DATABASE_URL."""
def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Connected to MQTT broker. Subscribing to '{MQTT_TOPIC}'...")
        client.subscribe(MQTT_TOPIC, qos=1)
    else:
        print(f"MQTT connection failed with code {rc}")

"""Called when a message arrives on the subscribed topic."""
def on_message(client, userdata, msg):
    global last_uptime_ms, last_wall_time

    try:
        payload = json.loads(msg.payload.decode())
        uptime_ms = payload.get("ts", 0)
        wh = payload.get("wh", 0)
        total_wh = payload.get("total_wh", 0)

        now = datetime.now(timezone.utc)

        if last_wall_time is None:
            # Very first message ever — anchor the timeline
            reading_time = now
            source = "live (first)"
        elif uptime_ms <= last_uptime_ms:
            # Device rebooted (uptime went backwards) — reset anchor
            reading_time = now
            source = "live (reboot)"
        else:
            # Normal case: compute time from uptime delta
            # If messages arrive in real-time, uptime delta ≈ wall-clock delta
            # If messages are buffered, uptime delta reflects the real spacing
            uptime_delta_ms = uptime_ms - last_uptime_ms
            reading_time = last_wall_time + timedelta(milliseconds=uptime_delta_ms)

            # Don't let computed time drift into the future
            if reading_time > now:
                reading_time = now

            source = "computed"

        # Update anchor
        last_uptime_ms = uptime_ms
        last_wall_time = reading_time

        print(f"Received ({source}): uptime={uptime_ms}ms, wh={wh}, "
              f"total_wh={total_wh}, time={reading_time.isoformat()}")

        # Insert with the computed timestamp
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO energy_readings (received_at, uptime_ms, wh, total_wh)
            VALUES (%s, %s, %s, %s)
            """,
            (reading_time, uptime_ms, wh, total_wh),
        )
        conn.commit()
        cur.close()
        conn.close()

        print(f"  -> Inserted into database")

    except json.JSONDecodeError:
        print(f"Invalid JSON: {msg.payload}")
    except Exception as e:
        print(f"Error processing message: {e}")

def main():
    print("MQTT-to-Postgres Bridge")
    print(f"MQTT: {MQTT_HOST}:{MQTT_PORT}")
    print(f"Topic: {MQTT_TOPIC}")
    print(f"Database: connected via DATABASE_URL")

    client = mqtt.Client()
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_connect
    client.on_message = on_message

    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            client.loop_forever()
        except Exception as e:
            print(f"Connection error: {e}. Retrying in 5s...")
            time.sleep(5)


if __name__ == "__main__":
    main()
