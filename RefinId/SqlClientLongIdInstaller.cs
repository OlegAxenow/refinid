using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using RefinId.InformationSchema;

namespace RefinId
{
	/// <summary>
	///     Installs <see cref="LongId" /> configuration for "System.Data.SqlClient" provider.
	/// </summary>
	/// <remarks>
	///     Configuration table stores information about last identifiers and types for each configured table.
	/// </remarks>
	public class SqlClientLongIdInstaller
	{
		private const string LongDbDataType = "bigint";
		private readonly IUniqueKeysProvider _keysProvider;
		private readonly TableCommandBuilder _tableCommandBuilder;

		/// <summary>
		///     Initializes <see cref="_tableCommandBuilder" /> with specified parameters.
		/// </summary>
		/// <param name="connectionString"> See <see cref="TableCommandBuilder" /> for details..</param>
		/// <param name="keysProvider"></param>
		/// <param name="tableName"> See <see cref="TableCommandBuilder" /> for details.</param>
		public SqlClientLongIdInstaller(string connectionString, IUniqueKeysProvider keysProvider, string tableName = null)
		{
			if (keysProvider == null) throw new ArgumentNullException("keysProvider");
			_keysProvider = keysProvider;
			_tableCommandBuilder = new TableCommandBuilder(connectionString, tableName,
				TableCommandBuilder.SqlProviderName);
		}

		/// <summary>
		///     Quoted table name for configuration.
		/// </summary>
		public string TableName
		{
			get { return _tableCommandBuilder.TableName; }
		}

		/// <summary>
		///     Installs necessary configuration into storage.
		/// </summary>
		/// <param name="shard"> Current shard id to be stored into <see cref="LongId.Shard" />.</param>
		/// <param name="reserved"> Reserved value to be stored into <see cref="LongId.Reserved" />.</param>
		/// <param name="useUniqueIfPrimaryKeyNotMatch"> Whether to use unique and primary keys or only primary keys.</param>
		/// <param name="tables"> Optional tables to be included into configuration.</param>
		public void Install(byte shard, byte reserved, bool useUniqueIfPrimaryKeyNotMatch, params Table[] tables)
		{
			using (var connection = (SqlConnection)_tableCommandBuilder.OpenConnection())
			{
				var commandBuilder = _tableCommandBuilder.GetDbCommandBuilder();
				SqlCommand command = connection.CreateCommand();

				var keys = new Dictionary<string, List<UniqueKey>>();
				foreach (UniqueKey key in _keysProvider.GetUniqueKeys(command))
				{
					if (!useUniqueIfPrimaryKeyNotMatch && !key.IsPrimaryKey) continue;
					
					string fullName = GetFullTableName(commandBuilder, key);
					List<UniqueKey> list;
					if (!keys.TryGetValue(fullName, out list))
						keys.Add(fullName, list = new List<UniqueKey>());
					list.Add(key);
				}

				RunTableCreation(command);
				if (tables == null) return;

				foreach (var table in tables)
				{
					string fullTableName = GetFullTableName(commandBuilder, table);
					// TODO: when table.KeyColumnName specified use ColumnsProvider.GetLongColums to find long identifier regardless unique or primary keys

					string targetColumnName = GetTargetColumnNameFromUniqueKeys(fullTableName, table, useUniqueIfPrimaryKeyNotMatch, keys);
					// TODO: add ColumnName to configuration
					// TODO: insert rows
				}
			}
			throw new NotImplementedException();
		}

		private static string GetTargetColumnNameFromUniqueKeys(string fullTableName, Table table, bool useUniqueIfPrimaryKeyNotMatch, Dictionary<string, List<UniqueKey>> keys)
		{
			List<UniqueKey> list;
			if (!keys.TryGetValue(fullTableName, out list))
				throw new ArgumentException(string.Format("No key constraint found for {0}.", fullTableName), "table");

			var longSingleColumnKeys = list.Where(x => x.ColumnCount == 1 &&
			                                           (x.IsPrimaryKey || useUniqueIfPrimaryKeyNotMatch) &&
			                                           x.DataType.Equals(LongDbDataType, StringComparison.OrdinalIgnoreCase))
				.OrderBy(x => !x.IsPrimaryKey)
				.ToArray();

			if (longSingleColumnKeys.Length == 0)
				throw new ArgumentException(string.Format("No key constraint with single {0} column found for {1}.",
					LongDbDataType, fullTableName), "table");

			if (!string.IsNullOrEmpty(table.KeyColumnName))
			{
				UniqueKey match = longSingleColumnKeys
					.FirstOrDefault(x => x.ColumnName.Equals(table.KeyColumnName, StringComparison.OrdinalIgnoreCase));
				if (match == null)
					throw new ArgumentException(string.Format("No key constraint with single {0} column found for {1}.{2}.",
						LongDbDataType, fullTableName, table.KeyColumnName), "table");
				return match.ColumnName;
			}

			if (longSingleColumnKeys.Length == 1 || longSingleColumnKeys[0].IsPrimaryKey)
				return longSingleColumnKeys[0].ColumnName;

			throw new ArgumentException(string.Format(@"Multiple key constraints with single {0} column found for {1}.
Use Table.KeyColumnName to specify desired column.", LongDbDataType, fullTableName), "table");
		}

		private static string GetFullTableName(DbCommandBuilder commandBuilder, ISchemaAndTable table)
		{
			return commandBuilder.QuoteIdentifier(table.Schema) + "." + commandBuilder.QuoteIdentifier(table.TableName);
		}

		private void RunTableCreation(SqlCommand command)
		{
			command.Run("IF OBJECT_ID('" + TableName + "') IS NULL " +
			            "CREATE TABLE " + TableName + " (" + TableCommandBuilder.TypeColumnName +
			            " smallint not null primary key, " + TableCommandBuilder.IdColumnName +
			            " bigint not null, " + TableCommandBuilder.TableNameColumnName +
			            " sysname null)");
		}
	}
}