CREATE TABLE bx1155 { schema { cstring named[11] } }$$

CREATE LUA AUDITED TRIGGER omar ON (TABLE bx1155 FOR INSERT) 

INSERT INTO bx1155 VALUES("0123456789")

SELECT sleep(1)

SELECT new_named FROM "$audit_bx1155"

DROP LUA TRIGGER omar 
DROP TABLE "$audit_bx1155"
DROP TABLE bx1155