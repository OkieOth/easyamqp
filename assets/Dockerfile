FROM rabbitmq:3.11.19-management-alpine as build

FROM python:3.10-slim-buster

COPY --from=build /usr/local/bin/rabbitmqadmin /bin/rabbitmqadmin

ENTRYPOINT [ "rabbitmqadmin"]