FROM ubuntu:noble

SHELL ["/bin/bash", "-c"]

RUN apt-get update
RUN apt-get -y install software-properties-common

RUN add-apt-repository ppa:openjdk-r/ppa 
RUN add-apt-repository ppa:swi-prolog/stable

RUN apt-get update
RUN apt-get -y install git
RUN apt-get -y install unzip
RUN apt-get -y install openjdk-8-jdk
RUN apt-get -y install swi-prolog
RUN apt-get -y install swi-prolog-java

RUN touch /root/.profile   

RUN echo 'export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib/swi-prolog/lib/x86_64-linux"' >> /root/.profile

RUN apt-get update
RUN apt-get -y install ant

# Add JAVA_HOME and ANT_HOME as they are needed during the compilation of TIMS.
RUN echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> /root/.profile
RUN echo 'export ANT_HOME=/opt/apache-ant-1.10.1' >> /root/.profile
RUN echo 'export PATH=${ANT_HOME}/bin:${JAVA_HOME}/bin:${PATH}' >> /root/.profile

RUN source /root/.profile

# Clone and create a local tims repository at user home directory.
RUN echo -e Cloning TIMS repository from GitHub...
RUN git clone https://github.com/bii-absd/tims --branch tims_agpl3 --single-branch /root/tims
RUN chown -R root:root /root/tims
RUN echo -e Building the project now...
WORKDIR /root/tims
RUN ant -Dlibs.CopyLibs.classpath=./libraries/org-netbeans-modules-java-j2seproject-copylibstask.jar

RUN apt-get update
RUN apt-get -y install postgresql

USER postgres
EXPOSE 5432
RUN service postgresql start && psql -U postgres -d postgres -c "ALTER USER postgres WITH PASSWORD 'tims2017';"

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
