# TL;DR;

This folder contains some `rabbitmqadmin` responses.

* [Connections](./get_connections_response.json)
* [Channels](./get_channels_response.json)
* [Queues](./get_queues_response.json)

# JPath Queries

docker run -d -p 8080:80 ashphy/jsonpath-online-evaluator:latest
(https://jsonpath.com/)

* Number of connections (works for all elem counts): `$.[*]`, and count the elems of the result
* Name of a connection with a specific connection name: 
  `$[?(@.client_properties.connection_name == 'NAME_TO_SEARCH')].name`

* Number of consumers on a queue with a specific name:
  `$[?(@.name == 'NAME_TO_SEARCH')].consumers`

* Nuber of channels per connection name: `$[?(@.connection_details.name == "CONN_NAME")]`, and count the elems of the result