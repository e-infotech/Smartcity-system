import sqlite3
import json
import paho.mqtt.client as mqtt
import requests
import random
import time
from datetime import datetime
client = None
# Thingsboard platform credentials
THINGSBOARD_HOST = '127.0.0.1'
ACCESS_TOKEN = 'FOIXtYC9Z3j4LwqQq86o' #smartcity sensor device

attributes = {
    'waveHeight': 0,
    'breakWater': 0,
    'temperature': 0,
    'coolingSystem':0
}
def get_timestamp():
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")

rpc_request = {"method": "getAttributes", "params": ""}
def temperature_high(temperature):
    return temperature >= 20

def waveHeight_high(waveHeight):
    return waveHeight >= 1

def update_attributes(client, data):
    result = client.publish('v1/devices/me/attributes', json.dumps(data), 2)
    if result[0] == 0:
        print("Attributes successfully published to Thingsboard")
    #print(data)

    
def coolingSystem(flag):
    
    #global temperature
    #temperature=attributes['temperature']
    if flag:
        print(flag)
        attributes['coolingSystem'] = 1
        


        attributes['temperature']-=1
        #attributes['temperature']= temperature
        print(attributes['temperature'])
        update_attributes(client, attributes)
        time.sleep(10)
        
        
   
    else:
        
        print(flag)
        attributes['coolingSystem'] = 0
        
        attributes['temperature'] += 1
        update_attributes(client, attributes)
        time.sleep(10)

def breakWater(flag):
    waveHeight=attributes['waveHeight']
    if flag:
        print(flag)
        attributes['breakWater'] = 1

        #waveHeight-=0.1
        attributes['waveHeight'] -= 0.1
        attributes['waveHeight']=float("{:.1f}".format(attributes['waveHeight']))
        print(attributes['waveHeight'])
        update_attributes(client, attributes)
        time.sleep(10)
        
   
    else:
        print(flag)
        attributes['breakWater'] = 0
        attributes['waveHeight'] = waveHeight
        update_attributes(client, attributes)

def on_connect(client, userdata, flags, rc):
    client.subscribe('v1/devices/me/rpc/request/+')
    client.publish('v1/devices/me/rpc/request/1', json.dumps(rpc_request), 1)

def on_message(client, userdata, msg):

    if msg.topic.startswith('v1/devices/me/rpc/response/'):
        
        data = json.loads(msg.payload)
        #print("in on message", data)
        if 'ss_temperature' in data['params']:
            attributes['temperature'] = json.loads(data['params']['ss_temperature'])
            #check_rules("temperature_high", attributes['temperature'])
        if 'ss_waveHeight' in data['params']:
            attributes['waveHeight'] = json.loads(data['params']['ss_waveHeight'])
            #print(attributes['waveHeight'])

    return attributes
        #print(attributes)
##        check_rules("temperature_high", attributes['temperature'])
##        check_rules("waveHeight_high", attributes['waveHeight'])
##        print("temperature:{:g}".format(attributes['temperature']))
##        print("waveHeight:{:g}".format(attributes['waveHeight']))
##        if 'breakWater' in data['params']:
##            attributes['breakWater'] = data['params']['breakWater']
##        if 'version' in data['params']:
##            attributes['version'] = data['params']['version']
        #update_attributes(client, attributes)


def get_attributes():
    global client 
    client = mqtt.Client()
    client.username_pw_set(ACCESS_TOKEN)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(THINGSBOARD_HOST, 1883, 60)
    client.loop_start()


##def update_waveheight(client, value):
##    attributes['waveHeight'] = value
##    update_attributes(client, attributes)
##
##def update_temperature(client, value):
##    attributes['temperature'] = value
##    update_attributes(client, attributes)


# Create a connection to the database
conn = sqlite3.connect('event_condition_action.db')

# Create a table to store events, conditions, and actions
##conn.execute('''CREATE TABLE ECA
##             (EVENT TEXT,
##             CONDITION TEXT,
##             ACTION TEXT);''')

def log_rule(event, condition, action):
    timestamp = get_timestamp()
    log_entry = f"{timestamp} - Event: {event}, Condition: {condition.__name__}, Action: {action.__name__}\n"
    with open('rule_log.txt', 'a') as log_file:
        log_file.write(log_entry)
        
# Define a function to receive events, conditions, and actions from the manager and save them to the database
def add_rule(event, condition, action):
    conn.execute("INSERT INTO ECA (EVENT, CONDITION, ACTION) VALUES (?, ?, ?)", (event, condition, action))
##    cur = conn.cursor()
##    cur.execute("SELECT * from ECA")
##    records = cur.fetchall()
##    for row in records:
##        print(row)
    conn.commit()
    print("Rule added to database: ", event, condition, action)

# Define a function to check for events and execute corresponding actions on the network
def check_rules(event, *args):
    rules = conn.execute("SELECT * FROM ECA WHERE EVENT = ?", (event,))
    for rule in rules:
        condition =globals() [rule[1]]
        action = globals() [rule[2]]
        print(rule[1])
        if condition(*args):  # evaluate the condition as a Python expression
            action(True)  # execute the action as a Python expression
            log_rule(event, condition, action)
            print("Action executed: ", rule[2])
        else:
            print("Condition is not met")
                

# Example usage
#add_rule("temperature_high", "temperature_high", "coolingSystem")
#add_rule("waveHeight_high", "waveHeight_high", "breakWater")


# Update telemetry
try:

    while True:
        
        get_attributes()
        #coolingSystem(True)
        #print("temperature:{:g}".format(attributes['temperature']))
        #print("waveHeight:{:g}".format(attributes['waveHeight']))
        #coolingSystem(True)
        #breakWater(True)
        check_rules("temperature_high", attributes['temperature'])
        check_rules("waveHeight_high", attributes['waveHeight'])



        time.sleep(10)

except KeyboardInterrupt:
    pass
