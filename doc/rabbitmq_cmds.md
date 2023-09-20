Some useful commands to investigate rabbitmq ..

```bash
# list current connections as JSON array
rabbitmqadmin --username guest --password guest list connections -f pretty_json

jq '.[] | select(.client_properties.connection_name == "playground") | .name' 


```