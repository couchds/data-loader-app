#!/bin/bash

# Basic script to set up MySQL test table. work in progress...

# MySQL creds
MYSQL_ROOT_USER="root"
MYSQL_ROOT_PASSWORD=""

TEST_DB="test_db"
TEST_USER="test_user"
TEST_PASSWORD="test_pass"

echo "Setting up MySQL integration test database..."

# Drop and recreate the test database
mysql -u$MYSQL_ROOT_USER -p$MYSQL_ROOT_PASSWORD <<EOF
DROP DATABASE IF EXISTS $TEST_DB;
CREATE DATABASE $TEST_DB;
DROP USER IF EXISTS '$TEST_USER'@'localhost';
CREATE USER '$TEST_USER'@'localhost' IDENTIFIED BY '$TEST_PASSWORD';
GRANT ALL PRIVILEGES ON $TEST_DB.* TO '$TEST_USER'@'localhost';
FLUSH PRIVILEGES;
EOF

echo "Test DB created successfully!"
