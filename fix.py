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

    def lookup(value1):
        try:
            y = json.loads(message)
            return float(y[value1])
        except:
            return None
    val1 = lookup('suhu1')
    val2 = lookup('kelembaban1')
    val3 = lookup('suhu2')
    val4 = lookup('kelembaban2')
    val5 = lookup('cahaya1')
    val6 = lookup('cahaya2')
    val7 = lookup('cahaya3')
    val8 = lookup('cahaya4')
    val9 = lookup('cahaya5')
    val10 = lookup('DO')
    val11 = lookup('TDS')
    val12 = lookup('Water')
    val13 = lookup('pH')
    val14 = lookup('DO1')
    val15 = lookup('TDS1')
    val16 = lookup('Water1')
    val17 = lookup('pH1')

    json_body = [
        {
            "measurement": msg.topic,
            "time": receiveTime,
            "fields": {
                "value1": val1,
                "value2": val2,
                "value3": val3,
                "value4": val4,
                "value5": val5,
                "value6": val6,
                "value7": val7,
                "value8": val8,
                "value9": val9,
                "value10": val10,
                "value11": val11,
                "value12": val12,
                "value13": val13,
                "value14": val14,
                "value15": val15,
                "value16": val16,
                "value17": val17
            }
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
