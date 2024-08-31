namespace DB_Schema_Export_Tool
{
    internal class SortKeyValue
    {
        /// <summary>
        /// True if the value is a number (integer or double)
        /// </summary>
        public bool IsNumeric { get; set; }

        /// <summary>
        /// Numeric value
        /// </summary>
        public double NumericValue { get; set; }

        /// <summary>
        /// Value
        /// </summary>
        public string Value { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="value">Value for this sort key</param>
        /// <param name="isNumeric">When true, try to convert it to a double</param>
        public SortKeyValue(string value, bool isNumeric)
        {
            Value = value;
            IsNumeric = isNumeric;

            if (isNumeric && double.TryParse(value, out var numericValue))
            {
                NumericValue = numericValue;
            }
        }

        /// <summary>
        /// Show the sort key value
        /// </summary>
        public override string ToString()
        {
            return Value;
        }
    }
}
