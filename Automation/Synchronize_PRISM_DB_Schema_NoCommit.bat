rem Can specify data tables for export using /data:F:\Cached_DBSchema\GigasaxTableData.txt 
rem Can auto-commit with /Commit

F:
cd "F:\My Documents\Projects\DataMining\Database_Tools\DB_Schema_Export_Tool\bin"

DB_Schema_Export_Tool.exe F:\Cached_DBSchema /server:Gigasax      /DBList:DMS_Capture,DMS_Data_Package,DMS_Pipeline,DMS5,Ontology_Lookup /sync:"F:\My Documents\Projects\DataMining\Database_Schema\DMS"         /Git /L /LogFolder:Logs /Data:..\Automation\DMS_Data_Tables.txt
DB_Schema_Export_Tool.exe F:\Cached_DBSchema /server:Proteinseqs  /DBList:Manager_Control,Protein_Sequences                              /sync:"F:\My Documents\Projects\DataMining\Database_Schema\DMS"         /Git /L /LogFolder:Logs /Data:..\Automation\ProteinSeqs_Data_Tables.txt

DB_Schema_Export_Tool.exe F:\Cached_DBSchema /server:Pogo         /DB:mt_template_01                                                /sync:"F:\My Documents\Projects\DataMining\Database_Schema\MTS\MT_Template"  /Git /L /LogFolder:Logs /NoSubfolder
DB_Schema_Export_Tool.exe F:\Cached_DBSchema /server:Pogo         /DB:pt_template_01                                                /sync:"F:\My Documents\Projects\DataMining\Database_Schema\MTS\PT_Template"  /Git /L /LogFolder:Logs /NoSubfolder
DB_Schema_Export_Tool.exe F:\Cached_DBSchema /server:Pogo         /DBList:mt_main,prism_rpt,prism_ifc,mts_master,MT_HistoricLog,dba /sync:"F:\My Documents\Projects\DataMining\Database_Schema\MTS"              /Git /L /LogFolder:Logs /Data:..\Automation\MTS_Data_Tables.txt
DB_Schema_Export_Tool.exe F:\Cached_DBSchema /server:ProteinSeqs2 /DBList:Master_Sequences,Master_Seq_Scratch                       /sync:"F:\My Documents\Projects\DataMining\Database_Schema\MTS"              /Git /L /LogFolder:Logs
