FROM ubuntu:noble

SHELL ["/bin/bash", "-c"]

RUN apt-get update
RUN apt-get -y install openjdk-8-jdk
RUN apt-get -y install libmariadb-java

RUN mkdir /var/cbioportal
RUN chown $USER:$USER /var/cbioportal
COPY src/cbioportal /var/cbioportal

COPY tims /root/tims

WORKDIR /opt
COPY src/cbioportal/apache-tomcat-8.0.45.tar.gz /opt/apache-tomcat-8.0.45.tar.gz
RUN tar xzf apache-tomcat-8.0.45.tar.gz
RUN mv /opt/apache-tomcat-8.0.45/ /opt/tomcat-8.0.45/
RUN chown -R $USER:$USER /opt/tomcat-8.0.45/

RUN cp /root/tims/scripts/tomcat-users.xml /opt/tomcat-8.0.45/conf
RUN cp /root/tims/scripts/context.xml /opt/tomcat-8.0.45/conf
RUN cp /var/cbioportal/cbioportal.war /opt/tomcat-8.0.45/webapps

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV ANT_HOME=/opt/apache-ant-1.10.1
ENV PATH=$ANT_HOME/bin:$JAVA_HOME/bin:$PATH

RUN /opt/tomcat-8.0.45/bin/catalina.sh start
