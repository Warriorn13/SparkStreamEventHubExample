import uuid
import datetime
import random
import json
from azure.servicebus import ServiceBusService

NAMESPACE='chen-test-eh'
KEY='ifs5GUcqdBANX8kh5T2DuwXrd6Qfhz1M5MaBtFIyLLY='
EVENTHUB='test_cap'

sbs=ServiceBusService(service_namespace=NAMESPACE, shared_access_key_name='RootManageSharedAccessKey', shared_access_key_value=KEY, host_base='.servicebus.chinacloudapi.cn')
devices = []
for x in range(0, 10):
  devices.append(str(uuid.uuid4()))

for y in range(0,20000):
  for dev in devices:
      reading = {'id': dev, 'timestamp': str(datetime.datetime.utcnow()), 'uv': random.random(), 'temperature': random.randint(70, 100), 'humidity': random.randint(70, 100)}
      s = json.dumps(reading)
      sbs.send_event(EVENTHUB, s)
  print (y)

# for y in range(0,200):
#    s='hello '*random.randint(1,10) + 'world '*random.randint(1,10)
#    sbs.send_event(EVENTHUB, s)
