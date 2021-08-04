SELECT "--------regular alter------"
CREATE TABLE test(c int)$$
CREATE LUA AUDITED TRIGGER bloop ON (TABLE test FOR INSERT)
INSERT INTO test VALUES(4)
ALTER TABLE test { schema { cstring c[5] int b null=yes } }$$
SELECT sleep(1)
SELECT new_c, new_b FROM "$audit_test" ORDER BY new_c

SELECT "--------chained alter------"
CREATE LUA AUDITED TRIGGER bloop2 ON (TABLE "$audit_test" FOR INSERT)
INSERT INTO test VALUES("a", 3)
ALTER TABLE test { schema { cstring c[5] int b null=yes cstring d[6] null=yes } }$$
INSERT INTO test VALUES("b", 7, "HEY")
SELECT sleep(1)
SELECT * FROM "$audit_$audit_test" ORDER BY new_new_c

SELECT "--------one carry delete------"
DROP LUA TRIGGER bloop2
ALTER TABLE TEST { schema { cstring c[5] cstring d[6] null=yes } }$$
INSERT INTO test VALUES("d", "WORLD")

DROP LUA TRIGGER bloop
DROP TABLE "$audit_$audit_test"
DROP TABLE "$audit_test"
DROP TABLE test