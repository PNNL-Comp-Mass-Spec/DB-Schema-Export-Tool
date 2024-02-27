namespace DB_Schema_Export_Tool
{
    internal class DatabaseObjectInfo
    {
        /// <summary>
        /// Object Schema
        /// </summary>
        public string Schema { get; set; }

        /// <summary>
        /// Object name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Object type
        /// </summary>
        public string Type { get; set; }

        /// <summary>
        /// Object owner
        /// </summary>
        public string Owner { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public DatabaseObjectInfo()
        {
            Clear();
        }

        /// <summary>
        /// Constructor that accepts name and type
        /// </summary>
        /// <param name="objectName">Object name</param>
        /// <param name="objectType">Object type</param>
        public DatabaseObjectInfo(string objectName, string objectType)
        {
            Clear();
            Name = objectName;
            Type = objectType;
            Owner = string.Empty;
        }

        /// <summary>
        /// Reset all properties to empty strings
        /// </summary>
        public void Clear()
        {
            Schema = string.Empty;
            Name = string.Empty;
            Type = string.Empty;
            Owner = string.Empty;
        }

        public override string ToString()
        {
            if (string.IsNullOrWhiteSpace(Type))
            {
                if (string.IsNullOrWhiteSpace(Schema))
                {
                    return Name;
                }

                return string.Format("{0}.{1}", Schema, Name);
            }

            return string.Format("{0}: {1}.{2}", Type, Schema, Name);
        }
    }
}
