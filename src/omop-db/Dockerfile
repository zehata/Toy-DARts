FROM ubuntu:noble

SHELL ["/bin/bash", "-c"]

RUN apt-get update
RUN apt-get -y install postgresql

USER postgres
RUN service postgresql start && psql -U postgres -d postgres -c "ALTER USER postgres WITH PASSWORD 'postgres';"

COPY src/omop-db/scripts /root/omop/scripts/
USER root
RUN cp /root/omop/scripts/pg_hba.conf /etc/postgresql/16/main/
RUN chown postgres:postgres /etc/postgresql/16/main/pg_hba.conf

RUN touch /root/.pgpass
RUN echo '*:*:*:postgres:postgres' > /root/.pgpass
RUN chmod 0600 /root/.pgpass
RUN chown root:root /root/.pgpass

RUN service postgresql restart && \
    psql -U postgres -c 'CREATE DATABASE omop WITH TEMPLATE = template0' && \
    psql -v ON_ERROR_STOP=1 -1 -d omop -U postgres -f /root/omop/scripts/OMOPCDM_postgresql_5.4_ddl.sql && \
    psql -v ON_ERROR_STOP=1 -1 -d omop -U postgres -f /root/omop/scripts/Bootstrap_Athena_standard_concepts.sql && \
    psql -v ON_ERROR_STOP=1 -1 -d omop -U postgres -f /root/omop/scripts/OMOPCDM_postgresql_5.4_primary_keys.sql && \
    psql -v ON_ERROR_STOP=1 -1 -d omop -U postgres -f /root/omop/scripts/OMOPCDM_postgresql_5.4_constraints.sql && \
    psql -v ON_ERROR_STOP=1 -1 -d omop -U postgres -f /root/omop/scripts/OMOPCDM_postgresql_5.4_indices.sql && \
    psql -v ON_ERROR_STOP=1 -1 -d omop -U postgres -f /root/omop/scripts/Athena_standard_concepts.sql

USER postgres
ENV PATH $PATH:/usr/lib/postgresql/16/bin
EXPOSE 5432
CMD ["postgres", "-i", "-D", "/etc/postgresql/16/main/"]