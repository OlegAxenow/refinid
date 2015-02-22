using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace RefinId.Metadata
{
	/// <summary>
	///     Provides information about primary and uniqu keys from INFORMATION_SCHEMA views.
	/// </summary>
	[SuppressMessage("StyleCop.CSharp.DocumentationRules", "SA1650:ElementDocumentationMustBeSpelledCorrectly", Justification = "Reviewed. Suppression is OK here.")]
	public class SQLiteDbMetadataProvider : IDbMetadataProvider
	{
		private const string AllTablesCommandText = @"SELECT name FROM sqlite_master WHERE type = 'table'";
		private const string ColumnsCommandTextPattern = @"PRAGMA table_info({0})";
		private const string TablesPattern = @"SELECT COUNT(*) FROM sqlite_master WHERE name = '{0}'";

		/// <summary>
		///     Returns <see cref="UniqueKey" /> instances for all unique and primary key constraints for current database,.
		/// </summary>
		public IEnumerable<UniqueKey> GetUniqueKeys(DbCommand command, string dataType)
		{
			if (command == null) throw new ArgumentNullException("command");
			if (command.Connection == null || command.Connection.State != ConnectionState.Open)
				throw new ArgumentException();

			command.CommandText = AllTablesCommandText;
			command.CommandType = CommandType.Text;
			var tables = new List<string>();

			using (DbDataReader reader = command.ExecuteReader())
			{
				while (reader.Read())
					tables.Add(reader.GetString(0));
			}

			foreach (var table in tables)
			{
				// TODO: cache ordinals and use private classes; we can use single var instead of list (and check for null)
				var keyColumns = new List<Tuple<string, string>>();

				command.CommandText = string.Format(ColumnsCommandTextPattern, table);
				using (DbDataReader reader = command.ExecuteReader())
				{
					while (reader.Read())
					{
						var keyOrdinal = reader.GetOrdinal("pk");
						var nameOrdinal = reader.GetOrdinal("name");
						var typeOrdinal = reader.GetOrdinal("type");

						if (reader.GetInt32(keyOrdinal) != 0)
							keyColumns.Add(new Tuple<string, string>(reader.GetString(nameOrdinal), reader.GetString(typeOrdinal)));
					}
				}

				if (keyColumns.Count != 1) continue;

				// TODO: refactor (and implement for non-int?)
				var keyColumn = keyColumns[0];
				if (dataType.IndexOf("INT", StringComparison.OrdinalIgnoreCase) >= 0 &&
					keyColumn.Item2.IndexOf("INT", StringComparison.OrdinalIgnoreCase) >= 0)
				{
					yield return new UniqueKey(string.Empty, table, keyColumn.Item1, true);
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