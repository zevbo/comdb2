CREATE TABLE t1(i int)$$
CREATE TABLE t2(b char(5))$$

CREATE LUA AUDITED TRIGGER too_greedy (TABLE t1 FOR INSERT) (TABLE t1 FOR DELETE)
SELECT * FROM comdb2_triggers

DROP TABLE t1 
DROP TABLE t2