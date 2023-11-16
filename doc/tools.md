Some useful commands to investigate rabbitmq ..
# Rabbitmq
```bash
# list current connections as JSON array
rabbitmqadmin --username guest --password guest list connections -f pretty_json

jq '.[] | select(.client_properties.connection_name == "con_test") | .name' 

rabbitmqadmin --username guest --password guest close connection name="172.31.0.1:51752 -> 172.31.0.2:5672"

rabbitmqadmin --username guest --password guest declare exchange name=first type=topic

rabbitmqadmin --username guest --password guest list exchanges -f pretty_json

```

## Curl

```bash
curl -i -u guest:guest -H "content-type:application/json" \
    -XDELETE http://localhost:15672/api/connections/172.22.0.1%3A45110%20-%3E%20172.22.0.2%3A5672
```

# JSON Parsing
https://jsonpath.com/
https://www.digitalocean.com/community/tutorials/python-jsonpath-examples