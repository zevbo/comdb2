PUT TUNABLE cary_alters_to_audits 0;

CREATE TABLE t(i int)$$
CREATE LUA AUDIT TRIGGER bloop ON (TABLE t FOR INSERT)
INSERT INTO t VALUES(4)
ALTER TABLE t ADD COLUMN b int$$
SELECT * FROM t ORDER BY i
SELECT new_i, old_i FROM "$audit_t" ORDER BY new_i, old_i

DROP TABLE t 
DROP TABLE "$audit_t"
DROP LUA TRIGGER bloop

PUT TUNABLE cary_alters_to_audits 1;

CREATE TABLE t(i int)$$
CREATE LUA AUDIT TRIGGER bloop ON (TABLE t FOR INSERT)
INSERT INTO t VALUES(4)
ALTER TABLE t ADD COLUMN b int$$
SELECT * FROM t
SELECT new_i, old_i, new_b, old_b FROM "$audit_t" ORDER BY new_i, old_i

DROP TABLE t 
DROP TABLE "$audit_t"
DROP LUA TRIGGER bloop

PUT TUNABLE cary_alters_to_audits 0;
