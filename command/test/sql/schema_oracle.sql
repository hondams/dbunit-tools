-- テーブル定義（Oracle）
CREATE TABLE dbunit_tools_users (
    id NUMBER(38) PRIMARY KEY,
    name VARCHAR2(100),
    email VARCHAR2(100)
);

CREATE TABLE dbunit_tools_products (
    id INT PRIMARY KEY,
    name VARCHAR2(100),
    price NUMBER(10,2)
);

