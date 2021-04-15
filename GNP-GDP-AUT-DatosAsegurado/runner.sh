#!/bin/sh
wget http://nexus.gnp.mx/repository/dlk_releases/mx/com/gnp/deployer/deployer/1/deployer-1.sh
mvn clean compile package -Dmaven.test.skip=true
bash deployer-1.sh