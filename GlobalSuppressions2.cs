// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Roslynator", "RCS1201:Use method chaining.", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:DB_Schema_Export_Tool.frmMain.ShowAboutBox")]
[assembly: SuppressMessage("Roslynator", "RCS1235:Optimize method call.", Justification = "Leave as-is since cannot implicitly convert from StringCollection to List<string>", Scope = "member", Target = "~M:DB_Schema_Export_Tool.DBSchemaExporterSQLServer.StringCollectionToList(System.Collections.Specialized.StringCollection)~System.Collections.Generic.IEnumerable{System.String}")]
