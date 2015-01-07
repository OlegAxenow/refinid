using System.Diagnostics.CodeAnalysis;

namespace RefinId.InformationSchema
{
	/// <summary>
	///     Contains properties with information about primary keys.
	/// </summary>
	public class UniqueKey : ISchemaAndTable
	{
		/// <summary>
		///     Initializes properties from parameters.
		/// </summary>
		public UniqueKey(string schema, string tableName, string columnName, bool isPrimaryKey, int columnCount,
			string dataType)
		{
			Schema = schema;
			TableName = tableName;
			ColumnName = columnName;
			IsPrimaryKey = isPrimaryKey;
			ColumnCount = columnCount;
			DataType = dataType;
		}

		/// <summary>
		///     Unquoted table's schema (e.g. "dbo").
		/// </summary>
		[SuppressMessage("StyleCop.CSharp.DocumentationRules", "SA1650:ElementDocumentationMustBeSpelledCorrectly", Justification = "Reviewed. Suppression is OK here.")]
		public string Schema { get; private set; }

		/// <summary>
		///     Unquoted table's name.
		/// </summary>
		public string TableName { get; private set; }

		/// <summary>
		///     Unquoted column's name.
		/// </summary>
		/// <remarks> If <see cref="ColumnCount" /> more than 1, single column (you can think that random) used to retrieve name.</remarks>
		public string ColumnName { get; private set; }

		/// <summary>
		///     Whether constraint is primary key or unique key.
		/// </summary>
		public bool IsPrimaryKey { get; private set; }

		/// <summary>
		///     Columns' count in primary key/unique key.
		/// </summary>
		public int ColumnCount { get; private set; }

		/// <summary>
		///     Columns' data type.
		/// </summary>
		public string DataType { get; private set; }
	}
}