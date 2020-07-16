rem Can specify data tables for export using /data:F:\Cached_DBSchema\GigasaxTableData.txt 
rem Can auto-commit with /Commit

F:
cd "F:\Documents\Projects\DataMining\Database_Tools\DB_Schema_Export_Tool\bin"

DB_Schema_Export_Tool.exe F:\Cached_DBSchema /server:Pogo         /DB:mt_template_01                                                /sync:"F:\Documents\Projects\DataMining\Database_Schema\MTS\MT_Template"  /Git /L /LogDir:Logs /NoSubdirectory
DB_Schema_Export_Tool.exe F:\Cached_DBSchema /server:Pogo         /DB:pt_template_01                                                /sync:"F:\Documents\Projects\DataMining\Database_Schema\MTS\PT_Template"  /Git /L /LogDir:Logs /NoSubdirectory
DB_Schema_Export_Tool.exe F:\Cached_DBSchema /server:Pogo         /DBList:mt_main,prism_rpt,prism_ifc,mts_master,MT_HistoricLog,dba /sync:"F:\Documents\Projects\DataMining\Database_Schema\MTS"              /Git /L /LogDir:Logs /Data:..\Automation\MTS_Data_Tables.txt
DB_Schema_Export_Tool.exe F:\Cached_DBSchema /server:Pogo         /DBList:Master_Sequences,Master_Seq_Scratch                       /sync:"F:\Documents\Projects\DataMining\Database_Schema\MTS"              /Git /L /LogDir:Logs