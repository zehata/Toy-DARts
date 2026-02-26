FROM ubuntu:noble

SHELL ["/bin/bash", "-c"]

RUN apt-get update
RUN apt-get -y install postgresql

USER postgres
EXPOSE 5432
RUN service postgresql start && psql -U postgres -d postgres -c "ALTER USER postgres WITH PASSWORD 'tims2017';"

COPY tims/scripts/ /root/tims/scripts/
USER root
RUN cp /root/tims/scripts/pg_hba.conf /etc/postgresql/16/main/
RUN chown postgres:postgres /etc/postgresql/16/main/pg_hba.conf

RUN touch /root/.pgpass
RUN echo '*:*:*:postgres:tims2017' > /root/.pgpass
RUN chmod 0600 /root/.pgpass
RUN chown root:root /root/.pgpass

RUN service postgresql restart && \
    psql -U postgres -c 'CREATE DATABASE tims WITH TEMPLATE = template0' && \
    psql -d tims -U postgres < /root/tims/scripts/tims_database_creation.sql && \
    psql -d tims -U postgres -f /root/tims/scripts/tims_database_setup.sql