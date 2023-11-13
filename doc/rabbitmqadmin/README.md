# TL;DR;

This folder contains some `rabbitmqadmin` responses.

* [Connections](./get_connections_response.json)
* [Channels](./get_channels_response.json)
* [Queues](./get_queues_response.json)

# JPath Queries

(https://jsonpath.com/)

* Number of Connections (works for all elem counts): `$.[*].length()`
* Name of a connection with a specific connection name: 
  `$[?(@.client_properties.connection_name == 'NAME_TO_SEARCH')].name`
* Number of consumers on a queue with a specific name:
  `$[?(@.name == 'NAME_TO_SEARCH')].consumers`