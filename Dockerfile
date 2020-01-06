FROM mysql:latest
ENV MYSQL_ROOT_PASSWORD root
ENV MYSQL_DATABASE sakila
ENV MYSQL_USER root
COPY ./sakila.sql /docker-entrypoint-initdb.d/sakila.sql
EXPOSE 3306
