CREATE TABLE t(i int, s char(65))$$

CREATE LUA AUDITED TRIGGER shoutout_to_monse ON (TABLE t FOR INSERT)  

INSERT INTO t values(4)
INSERT INTO t values(5, "abc")

SELECT sleep(1)

SELECT * FROM "$audit_t" ORDER BY logtime

DROP TABLE t
DROP TABLE "$audit_t"
DROP LUA TRIGGER shoutout_to_monse