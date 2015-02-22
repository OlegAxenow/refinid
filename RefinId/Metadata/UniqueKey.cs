using System.Diagnostics.CodeAnalysis;

namespace RefinId.Metadata
{
	/// <summary>
	///     Contains properties with information about primary keys.
	/// </summary>
	public class UniqueKey : ISchemaAndTable
	{
		/// <summary>
		///     Initializes properties from parameters.
		/// </summary>
		public UniqueKey(string schema, string tableName, string columnName, bool isPrimaryKey)
		{
			Schema = schema;
			TableName = tableName;
			ColumnName = columnName;
			IsPrimaryKey = isPrimaryKey;
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
		public string ColumnName { get; private set; }

		/// <summary>
		///     Whether constraint is primary key or unique key.
		/// </summary>
		public bool IsPrimaryKey { get; private set; }
	}
}