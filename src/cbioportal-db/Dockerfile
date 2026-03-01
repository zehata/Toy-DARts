FROM ubuntu:trusty

SHELL ["/bin/bash", "-c"]

RUN apt-get update
RUN apt-get -y install mysql-server-5.6 mysql-client-5.6 && echo ''
RUN apt-get -y install wget

RUN service mysql start && \
    mysql -u root -e "CREATE DATABASE cbioportal16;" && \
    mysql -u root -e "CREATE USER 'cbio_user'@'localhost' IDENTIFIED BY 'password';" && \
    mysql -u root -e "GRANT ALL ON cbioportal16.* TO 'cbio_user'@'localhost';" && \
    mysql -u root -e "FLUSH PRIVILEGES;"

RUN mkdir /root/sql
WORKDIR /root/sql
RUN wget http://mendel.bii.a-star.edu.sg/SEQUENCES/TIMS/cgds.sql
RUN wget http://mendel.bii.a-star.edu.sg/SEQUENCES/TIMS/seed-cbioportal_hg19_v2.6.0.sql.gz
RUN gunzip seed-cbioportal_hg19_v2.6.0.sql.gz

RUN service mysql start && \
    mysql -u cbio_user -ppassword cbioportal16 < /root/sql/cgds.sql && \
    mysql -u cbio_user -ppassword cbioportal16 < /root/sql/seed-cbioportal_hg19_v2.6.0.sql