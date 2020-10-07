import paho.mqtt.client as mqtt
import json
import csv
import pandas as pd
import os

all_rows_ids = []
try:
    df = pd.read_csv('some.csv',encoding='ansi')
    all_rows_ids = list(df['row'])
except:
    pass

filename = 'some.csv'
if os.path.exists(filename):
    append_write = 'a' # append if already exists
else:
    append_write = 'w' # make a new file if not

csv_file = open(filename,append_write)


writer = None
first_pass = True



# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("tnt")



# The callback for when the client receives a CONNACK response from the server.
def on_disconnect(client, userdata, rc):
    if rc != 0:
        print(f"Unexpected disconnection: {rc}")
       


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    global all_rows_ids
    global csv_file
    global writer
    global first_pass

    current_row    = json.loads(msg.payload)
    current_row_id = current_row['row']
    if len(all_rows_ids) >= 17016:
        client.disconnect()
        return
    if first_pass: 
        first_pass = False
        field_names = list(current_row.keys())
        writer = csv.DictWriter(csv_file, fieldnames=field_names)
        if len(all_rows_ids) == 0:
            writer.writeheader()        

    if current_row_id in all_rows_ids:
        return
    all_rows_ids.append(current_row_id)
    writer.writerow(current_row)
    print(f'{msg.topic} {current_row_id}: {current_row}')

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect
client.username_pw_set('maratoners','ndsjknvkdnvjsbvj')
client.reconnect_delay_set(1,120)
print(client.connect("tnt-iot.maratona.dev", 30573, 60))
#print(client.subscribe("tnt"))

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
csv_file.close()
print(f'coletados {len(all_rows_ids)} registros')



