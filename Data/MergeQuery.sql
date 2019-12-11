-- This is compatible with SQL Server

MERGE mc.t_mgr_types AS t
USING (
Select 1 As mt_type_id, 'Analysis' As mt_type_name, 0 As mt_active
Union Select 2, 'Archive', 0
Union Select 3, 'Archive Verify', 0
Union Select 4, 'Capture', 0
Union Select 5, 'Data Extraction', 0
Union Select 6, 'Preparation', 0
Union Select 7, 'Analysis Results Transfer', 0
Union Select 8, 'Space', 1
Union Select 9, 'Data Import', 1
Union Select 10, 'Inst Data File Util', 0
Union Select 11, 'Analysis Tool Manager', 1
Union Select 12, 'FolderCreate', 1
Union Select 13, 'Inst Dir Scanner', 1
Union Select 14, 'StatusMsgDBUpdater', 1
Union Select 15, 'CaptureTaskManager', 1
) as s
ON ( t.mt_type_id = s.mt_type_id)
WHEN MATCHED AND (
    t.mt_type_name <> s.mt_type_name OR
    ISNULL( NULLIF(t.mt_active, s.mt_active),
            NULLIF(s.mt_active, t.mt_active)) IS NOT NULL
    )
THEN UPDATE SET
    mt_type_name = s.mt_type_name,
    mt_active = s.mt_active
WHEN NOT MATCHED BY TARGET THEN
    INSERT(mt_type_id, mt_type_name, mt_active)
    VALUES(s.mt_type_id, s.mt_type_name, s.mt_active)
-- WHEN NOT MATCHED BY SOURCE THEN DELETE
;


SELECT * FROM mc.t_mgr_types Where mt_type_id <= 15
--SELECT * FROM Manager_Control.dbo.T_MgrTypes