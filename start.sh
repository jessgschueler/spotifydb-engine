docker run --name mariadb \
    -p 127.0.0.1:3306:3306 \
    -e MYSQL_ROOT_PASSWORD=mysql \
    -e MYSQL_DATABASE=spotify \
    --rm -d \
    mariadb:latest