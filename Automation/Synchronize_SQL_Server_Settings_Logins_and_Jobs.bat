@echo off

F:
pushd "F:\Documents\Projects\DataMining\Database_Tools\DB_Schema_Export_Tool\bin"

@echo on
DB_Schema_Export_Tool.exe F:\Cached_DBSchema /server:Gigasax      /ServerInfo /DBList:master /sync:"F:\Documents\Projects\DataMining\Database_Schema\Server_Config_Info\Gigasax"     /Git /L /LogDir:Logs
DB_Schema_Export_Tool.exe F:\Cached_DBSchema /server:Proteinseqs  /ServerInfo /DBList:master /sync:"F:\Documents\Projects\DataMining\Database_Schema\Server_Config_Info\ProteinSeqs" /Git /L /LogDir:Logs
DB_Schema_Export_Tool.exe F:\Cached_DBSchema /server:Pogo         /ServerInfo /DBList:master /sync:"F:\Documents\Projects\DataMining\Database_Schema\Server_Config_Info\Pogo"        /Git /L /LogDir:Logs

popd

@echo off
echo.
echo Use Beyond Compare to synchronize the following directories
echo "F:\Cached_DBSchema\ServerSchema__Gigasax       with  F:\Documents\Projects\DataMining\Database_Schema\Server_Config_Info\Gigasax"
echo "F:\Cached_DBSchema\ServerSchema__Proteinseqs   with  F:\Documents\Projects\DataMining\Database_Schema\Server_Config_Info\Proteinseqs"
echo "F:\Cached_DBSchema\ServerSchema__Pogo          with  F:\Documents\Projects\DataMining\Database_Schema\Server_Config_Info\Pogo"

"C:\Program Files\Beyond Compare 4\BCompare.exe" "F:\Cached_DBSchema\ServerSchema__Gigasax"     "F:\Documents\Projects\DataMining\Database_Schema\Server_Config_Info\Gigasax"
"C:\Program Files\Beyond Compare 4\BCompare.exe" "F:\Cached_DBSchema\ServerSchema__Proteinseqs" "F:\Documents\Projects\DataMining\Database_Schema\Server_Config_Info\Proteinseqs"
"C:\Program Files\Beyond Compare 4\BCompare.exe" "F:\Cached_DBSchema\ServerSchema__Pogo"        "F:\Documents\Projects\DataMining\Database_Schema\Server_Config_Info\Pogo"

echo.
echo "For PostgreSQL, run Synchronize_PostgreSQL_DB_Schema_PrismDB1.bat then synchronize these directories"
echo "F:\Documents\Projects\DataMining\Database_Schema\DMS_PgSQL\dms"
echo "F:\Documents\Projects\DataMining\Database_Schema\Server_Config_Info\PrismDB1\DMS_PgSQL\dms"
echo.
echo "In Beyond Compare, filter using: _extension*;_server*;_user*"

pause
