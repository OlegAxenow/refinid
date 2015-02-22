using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace RefinId.Metadata
{
	/// <summary>
	///     Provides information about primary and unique keys from INFORMATION_SCHEMA views.
	/// </summary>
	[SuppressMessage("StyleCop.CSharp.DocumentationRules", "SA1650:ElementDocumentationMustBeSpelledCorrectly", Justification = "Reviewed. Suppression is OK here.")]
	public class DbMetadataProvider : IDbMetadataProvider
	{
		private const string UniqueKeysPattern = @"SELECT t.* FROM
(SELECT kcu.TABLE_SCHEMA AS SchemaName, kcu.TABLE_NAME AS TableName,
		MAX(kcu.COLUMN_NAME) AS ColumnName, tc.CONSTRAINT_TYPE AS ConstraintType
	FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
	INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
		ON kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
		AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA AND kcu.TABLE_NAME = tc.TABLE_NAME
 WHERE tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
 GROUP BY tc.CONSTRAINT_TYPE, kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.COLUMN_NAME
 HAVING COUNT(*) = 1) t
 INNER JOIN INFORMATION_SCHEMA.COLUMNS AS c 
	ON c.TABLE_SCHEMA = t.SchemaName AND c.TABLE_NAME = t.TableName AND c.COLUMN_NAME = t.ColumnName
 WHERE c.DATA_TYPE = '{0}'";

		private const string TablesPattern = @"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{0}'";

		private const string PrimaryKeyConstraintType = "PRIMARY KEY";

		private const int SchemaOrdinal = 0;

		private const int TableNameOrdinal = 1;

		private const int ColumnNameOrdinal = 2;

		private const int ConstraintTypeOrdinal = 3;

		/// <summary>
		///     Returns <see cref="UniqueKey" /> instances for all unique and primary key constraints for current database,.
		/// </summary>
		public IEnumerable<UniqueKey> GetUniqueKeys(DbCommand command, string dataType)
		{
			if (command == null) throw new ArgumentNullException("command");
			if (dataType == null) throw new ArgumentNullException("dataType");
			if (command.Connection == null || command.Connection.State != ConnectionState.Open)
				throw new ArgumentException();

			command.CommandText = string.Format(UniqueKeysPattern, dataType);
			command.CommandType = CommandType.Text;

			using (DbDataReader reader = command.ExecuteReader())
			{
				while (reader.Read())
				{
					yield return new UniqueKey(
						reader.GetString(SchemaOrdinal), reader.GetString(TableNameOrdinal), reader.GetString(ColumnNameOrdinal),
						reader.GetString(ConstraintTypeOrdinal).Equals(PrimaryKeyConstraintType, StringComparison.OrdinalIgnoreCase));
				}
			}
		}

		/// <summary>
		/// Implements <see cref="IDbMetadataProvider.TableExists"/>.
		/// </summary>
		public bool TableExists(DbCommand command, string tableName)
		{
			var rowCount = command.Run(string.Format(TablesPattern, tableName), true);
			return rowCount != null && Convert.ToInt32(rowCount) == 1;
		}

		/// <summary>
		/// Implements <see cref="IDbMetadataProvider.GetParameterName"/>.
		/// </summary>
		public string GetParameterName(string invariantName)
		{
			if (invariantName == null) throw new ArgumentNullException("invariantName");
			return "@" + invariantName;
		}
	}
}