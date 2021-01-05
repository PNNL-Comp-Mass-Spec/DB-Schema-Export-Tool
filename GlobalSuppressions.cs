// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Silently ignore this exception", Scope = "member", Target = "~M:DB_Schema_Export_Tool.DBSchemaExporterBase.SetLocalError(DB_Schema_Export_Tool.DBSchemaExporterBase.DBSchemaExportErrorCodes,System.String,System.Exception)")]
[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Silently ignore this exception", Scope = "member", Target = "~M:DB_Schema_Export_Tool.DBSchemaExporterPostgreSQL.ConnectToPgServer(System.String)~System.Boolean")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Keep for readability", Scope = "member", Target = "~M:DB_Schema_Export_Tool.DBSchemaExporterBase.ComputeIncrementalProgress(System.Single,System.Single,System.Single)~System.Single")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:DB_Schema_Export_Tool.DBSchemaExporterPostgreSQL.StoreCachedLinesForObject(System.Collections.Generic.IDictionary{System.String,System.Collections.Generic.List{System.String}},System.Collections.Generic.List{System.String},System.String,DB_Schema_Export_Tool.DatabaseObjectInfo)")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:DB_Schema_Export_Tool.DBSchemaExportTool.FilesDiffer(System.IO.FileInfo,System.IO.FileInfo,DB_Schema_Export_Tool.DBSchemaExportTool.DifferenceReasonType@)~System.Boolean")]
[assembly: SuppressMessage("Simplification", "RCS1179:Unnecessary assignment.", Justification = "Keep for readability", Scope = "member", Target = "~M:DB_Schema_Export_Tool.DBSchemaExporterPostgreSQL.ExportDBTableData(System.String,DB_Schema_Export_Tool.TableDataExportInfo,System.Int64,DB_Schema_Export_Tool.WorkingParams)~System.Boolean")]
[assembly: SuppressMessage("Usage", "RCS1146:Use conditional access.", Justification = "Keep for readability", Scope = "member", Target = "~M:DB_Schema_Export_Tool.DBSchemaExporterSQLServer.ExportDBTableData(System.String,DB_Schema_Export_Tool.TableDataExportInfo,System.Int64,DB_Schema_Export_Tool.WorkingParams)~System.Boolean")]
