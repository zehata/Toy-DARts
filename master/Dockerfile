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

ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/lib/swi-prolog/lib/x86_64-linux

RUN apt-get update
RUN apt-get -y install ant

# Add JAVA_HOME and ANT_HOME as they are needed during the compilation of TIMS.
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV ANT_HOME=/opt/apache-ant-1.10.1
ENV PATH=$ANT_HOME/bin:$JAVA_HOME/bin:$PATH

# Clone and create a local tims repository at user home directory.
COPY tims /root/tims/
RUN chown -R root:root /root/tims
RUN echo -e Building the project now...
WORKDIR /root/tims
RUN ant -Dlibs.CopyLibs.classpath=./libraries/org-netbeans-modules-java-j2seproject-copylibstask.jar

RUN apt-get -y install wget

WORKDIR /opt
RUN wget http://download.java.net/glassfish/4.1/release/glassfish-4.1.zip
RUN unzip glassfish-4.1.zip
RUN rm glassfish-4.1.zip

RUN chgrp -R root /opt/glassfish4/glassfish
RUN chown -R root /opt/glassfish4/glassfish
RUN cp ~/tims/libraries/postgresql-9.4-1202.jdbc41.jar /opt/glassfish4/glassfish/lib/

ENV PORTAL_HOME=/var/cbioportal
ENV CATALINA_HOME=/opt/tomcat-8.0.45
ENV PATH=/opt/glassfish4/bin:$PATH

RUN mkdir /var/TIMS
RUN mkdir /var/TIMS/users
RUN mkdir /var/TIMS/users/tims-admin
RUN mkdir /var/TIMS/users/tims-pi
RUN mkdir /var/TIMS/users/tims-admin/output
RUN mkdir /var/TIMS/users/tims-admin/config
RUN mkdir /var/TIMS/users/tims-admin/log
RUN mkdir /var/TIMS/users/tims-pi/output
RUN mkdir /var/TIMS/users/tims-pi/config
RUN mkdir /var/TIMS/users/tims-pi/log
RUN chown -R $USER:$USER /var/TIMS

RUN cd /opt
RUN wget http://mendel.bii.a-star.edu.sg/SEQUENCES/TIMS/apache-tomcat-8.0.45.tar.gz
RUN tar xzf apache-tomcat-8.0.45.tar.gz
RUN rm apache-tomcat-8.0.45.tar.gz
RUN mv /opt/apache-tomcat-8.0.45/ /opt/tomcat-8.0.45/
RUN chown -R $USER:$USER /opt/tomcat-8.0.45/

COPY --chmod=+x master /root/master/