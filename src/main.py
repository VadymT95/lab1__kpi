from paho.mqtt import client as mqtt_client
import json
import time
from schema.aggregated_data_schema import AggregatedDataSchema
from schema.parking_schema import ParkingSchema
from file_datasource import FileDatasource
import config

def connect_mqtt(broker, port):
    """Create MQTT client"""
    print(f"CONNECT TO {broker}:{port}")

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print(f"Connected to MQTT Broker ({broker}:{port})!")
        else:
            print("Failed to connect {broker}:{port}, return code %d\n", rc)
            exit(rc)  # Stop execution

    client = mqtt_client.Client()
    client.on_connect = on_connect
    client.connect(broker, port)
    client.loop_start()
    return client

def publish(mqtt_client, topic_list, data_source, pause_duration):
    # Initialize the reading process from the data source
    data_source.startReading()
    while True:
        time.sleep(pause_duration)
        # Read data in batches from the data source
        for data_aggregated, data_parking in data_source.process(5):
            # Serialize the data to a format suitable for MQTT publishing
            serialized_data = [
                AggregatedDataSchema().dumps(data_aggregated),
                ParkingSchema().dumps(data_parking)]
            # Publish serialized data to each corresponding topic
            for topic, payload in zip(topic_list, serialized_data):
                # Attempt to publish and store the status
                publish_status = mqtt_client.publish(topic, payload)
                is_successful = publish_status[0]
                # Check if the publish operation was not successful
                if is_successful != 0:  # Non-zero indicates failure
                    print(f"Failed to send message to topic {topic}")
                # Optional: Uncomment to log successful sends
                # else:
                #     print(f"Message sent to topic {topic}")     

def run():
    # Prepare mqtt client
    client = connect_mqtt(config.MQTT_BROKER_HOST, config.MQTT_BROKER_PORT)
    # Prepare datasource
    datasource = FileDatasource("data/accelerometer.csv", "data/gps.csv", "data/parking.csv")
    # Infinity publish data
    publish(client, config.MQTT_TOPICS.split(','), datasource, config.DELAY)

if __name__ == "__main__":
    run()
