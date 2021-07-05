CREATE TABLE t(num int)$$

CREATE LUA TRIGGER t_audit ON (TABLE t FOR INSERT) AUDITED

SELECT "------table and procedure creation------"
SELECT * FROM comdb2_tables ORDER BY tablename

INSERT INTO t VALUES(5)
INSERT INTO t VALUES(10)
DELETE FROM t WHERE num=10

SELECT sleep(1)

SELECT "------table filling------"
SELECT * from "$audit_t" ORDER BY new_num

DROP TABLE t 
DROP LUA TRIGGER t_audit
DROP TABLE "$audit_t"