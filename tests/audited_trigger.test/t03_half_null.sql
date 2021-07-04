CREATE TABLE t(i int, s char(65))$$

CREATE LUA TRIGGER shoutout_to_monse ON (TABLE t FOR INSERT) AUDITED 

INSERT INTO t values(4)
INSERT INTO t values(5, "abc")

SELECT * FROM "$audit_t"

DROP TABLE t
DROP TABLE "$audit_t"
DROP LUA TRIGGER shoutout_to_monse