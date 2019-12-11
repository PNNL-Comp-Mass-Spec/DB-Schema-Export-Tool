-- One row at a time
INSERT INTO mc.t_mgr_types (mt_type_id, mt_type_name, mt_active)
VALUES (1, 'Analysis', 0)
ON CONFLICT(mt_type_id)
DO UPDATE SET
  mt_type_name = 'Anaylsis',
  mt_active = 0;


-- Multiple rows
INSERT INTO mc.t_mgr_types (mt_type_id, mt_type_name, mt_active)
OVERRIDING SYSTEM VALUE
VALUES
  (1, 'Analysis', 0),
  (2, 'Archive', 0),
  (3, 'Archive Verify', 0),
  (4, 'Capture', 0),
  (5, 'Data Extraction', 0),
  (6, 'Preparation', 0),
  (7, 'Analysis Results Transfer', 0),
  (8, 'Space', 1),
  (9, 'Data Import', 1),
  (10, 'Inst Data File Util', 0),
  (11, 'Analysis Tool Manager', 1),
  (12, 'FolderCreate', 1),
  (13, 'Inst Dir Scanner', 1),
  (14, 'StatusMsgDBUpdater', 1),
  (15, 'CaptureTaskManager', 1)
ON CONFLICT (mt_type_id)
DO UPDATE SET
  mt_type_name = EXCLUDED.mt_type_name,
  mt_active = EXCLUDED.mt_active;

SELECT * FROM mc.t_mgr_types Where mt_type_id <= 15

"-- Set the sequence's current value to the maximum current ID
"SELECT setval('mc.t_mgr_types_mt_type_id_seq', (SELECT MAX(mt_type_id) FROM mc.t_mgr_types));
"
"-- Preview the ID that will be assigned to the next item
"SELECT currval('mc.t_mgr_types_mt_type_id_seq');
