SET enable_planner_v2=1;
DROP USER IF EXISTS 'test-user';
DROP USER IF EXISTS 'test-priv';
DROP DATABASE IF EXISTS a;
DROP DATABASE IF EXISTS b;
DROP ROLE IF EXISTS 'test-role';
DROP ROLE IF EXISTS 'test';
DROP USER IF EXISTS 'test-user';
DROP USER IF EXISTS 'test-priv';
CREATE DATABASE a;
CREATE DATABASE b;

REVOKE ROLE 'test' FROM 'test-user'; -- {ErrorCode 2201}
CREATE USER 'test-user' IDENTIFIED BY 'password';
REVOKE ROLE 'test' FROM 'test-user';
CREATE ROLE 'test';
--REVOKE ROLE TEST
GRANT SELECT ON b.* TO ROLE 'test';
SHOW GRANTS FOR ROLE 'test';
GRANT ROLE 'test' TO 'test-user';
SHOW GRANTS FOR 'test-user';
REVOKE SELECT ON b.* FROM ROLE 'test';
SHOW GRANTS FOR 'test-user';
SHOW GRANTS FOR ROLE 'test';

REVOKE ROLE 'test' FROM ROLE 'test-role'; -- {ErrorCode 2204}
CREATE ROLE 'test-role';
REVOKE ROLE 'test' FROM ROLE 'test-role';

--REVOKE PRIV TEST
CREATE USER 'test-priv' IDENTIFIED BY 'A';
GRANT SELECT ON b.* TO 'test-priv';
SHOW GRANTS FOR 'test-priv';
REVOKE SELECT ON a.* FROM 'test-priv';
SHOW GRANTS FOR 'test-priv';
REVOKE SELECT ON b.* FROM 'test-priv';
SHOW GRANTS FOR 'test-priv';

DROP ROLE 'test-role';
DROP USER 'test-user';
DROP USER 'test-priv';
DROP DATABASE a;
DROP DATABASE b;
SET enable_planner_v2=0;
