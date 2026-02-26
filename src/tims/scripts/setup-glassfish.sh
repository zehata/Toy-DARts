#!/bin/bash

asadmin start-domain
asadmin create-jdbc-connection-pool --datasourceclassname org.postgresql.ds.PGConnectionPoolDataSource --restype javax.sql.ConnectionPoolDataSource --property portNumber=5432:password=tims2017:user=postgres:serverName=localhost:databaseName=tims tims_conn_pool
asadmin create-jdbc-resource --connectionpoolid tims_conn_pool jdbc/tims_conn_pool
asadmin set configs.config.server-config.network-config.network-listeners.network-listener.http-listener-1.port=8081
asadmin delete-jvm-options -Xmx512m
asadmin create-jvm-options -

asadmin deploy ~/tims/dist/TIMS.war & /opt/tomcat-8.0.45/bin/catalina.sh start