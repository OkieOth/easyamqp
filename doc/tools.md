Some useful commands to investigate rabbitmq ..
# Rabbitmq
```bash
# list current connections as JSON array
rabbitmqadmin --username guest --password guest list connections -f pretty_json

jq '.[] | select(.client_properties.connection_name == "con_test") | .name' 

rabbitmqadmin --username guest --password guest close connection name="172.31.0.1:51752 -> 172.31.0.2:5672"

```

# JSON Parsing
https://jsonpath.com/
https://www.digitalocean.com/community/tutorials/python-jsonpath-examples