FROM ubuntu:noble

SHELL ["/bin/bash", "-c"]

RUN apt-get update
RUN apt-get -y install libmariadb-java wget

RUN mkdir /var/cbioportal
RUN chown $USER:$USER /var/cbioportal
COPY src/cbioportal /var/cbioportal

COPY tims /root/tims

WORKDIR /opt
RUN wget http://mendel.bii.a-star.edu.sg/SEQUENCES/TIMS/apache-tomcat-8.0.45.tar.gz
RUN tar xzf apache-tomcat-8.0.45.tar.gz
RUN mv /opt/apache-tomcat-8.0.45/ /opt/tomcat-8.0.45/
RUN chown -R $USER:$USER /opt/tomcat-8.0.45/

RUN cp /root/tims/scripts/tomcat-users.xml /opt/tomcat-8.0.45/conf
RUN cp /root/tims/scripts/context.xml /opt/tomcat-8.0.45/conf
RUN cp /var/cbioportal/cbioportal.war /opt/tomcat-8.0.45/webapps
