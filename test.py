import paho.mqtt.client as mqtt
import datetime
import time
from influxdb import InfluxDBClient
import json


def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("huasengboay/#")


def on_message(client, userdata, msg):
    print("Received a message on topic: " + msg.topic)
    # Use utc as timestamp
    receiveTime = datetime.datetime.utcnow()
    message = msg.payload.decode("utf-8")
    print(message)
    data = {}
    y = json.loads(message)

    def lookup(value1, value2):
        try:
            data[value1] = value2
        except:
            return None
    lookup('value1', y(['suhu1']))
    lookup('value2', y(['kelembaban1']))
    lookup('value3', y(['suhu2']))
    lookup('value4', y(['kelembaban2']))
    lookup('value5', y(['cahaya1']))
    lookup('value6', y(['cahaya2']))
    lookup('value7', y(['cahaya3']))
    lookup('value8', y(['cahaya4']))
    lookup('value9', y(['cahaya5']))
    lookup('value10', y(['DO']))
    lookup('value11', y(['TDS']))
    lookup('value12', y(['Water']))
    lookup('value13', y(['pH']))
    print(data)
    json_body = [
        {
            "measurement": msg.topic,
            "time": receiveTime,
            "fields": data
        }
    ]

    dbclient.write_points(json_body)
    print("Finished writing to InfluxDB")


# Set up a client for InfluxDB
dbclient = InfluxDBClient('localhost', 8086, 'root', 'root', 'sensors')

# Initialize the MQTT client that should connect to the Mosquitto broker
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
connOK = False
while(connOK == False):
    try:
        client.connect("broker.hivemq.com", 1883, 60)
        connOK = True
    except:
        connOK = False
    time.sleep(5)

client.loop_forever()
