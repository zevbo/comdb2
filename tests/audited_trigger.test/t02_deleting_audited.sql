CREATE TABLE ghost(i char(6))$$

CREATE LUA TRIGGER hi_helena ON (TABLE ghost FOR INSERT) AUDITED

DROP TABLE "$audit_ghost"

INSERT INTO ghost values("bloop")

DROP TABLE ghost
DROP LUA TRIGGER hi_helena