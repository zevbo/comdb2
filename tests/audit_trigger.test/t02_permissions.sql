CREATE TABLE t(i int)$$
CREATE LUA AUDIT TRIGGER bloop ON (TABLE t FOR INSERT)
SELECT "-----------permission denied----------"
ALTER TABLE "$audit_t" ADD COLUMN b int$$
ALTER TABLE "$audit_t" { schema { int b } }$$
DROP TABLE "$audit_t"$$
ALTER TABLE "$audit_t" RENAME TO blooper$$
ALTER TABLE "$audit_t" DROP COLUMN old_i$$
ALTER TABLE "$audit_t" ADD UNIQUE INDEX "hello" (new_i, old_i)$$

DROP LUA TRIGGER bloop

SELECT sleep(1)
SELECT "-----------permission approved----------"
ALTER TABLE "$audit_t" ADD COLUMN b int$$
ALTER TABLE "$audit_t" RENAME TO blooper$$
ALTER TABLE blooper DROP COLUMN old_i$$
ALTER TABLE blooper ADD UNIQUE INDEX "hello" (new_i, old_i)$$
ALTER TABLE blooper { schema { int b } }$$

DROP TABLE t 
DROP TABLE blooper

