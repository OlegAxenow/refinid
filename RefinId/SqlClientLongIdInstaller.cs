using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
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
		private const int SysNameSize = 128;
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

				InsertConfiguration(useUniqueIfPrimaryKeyNotMatch, tables, command, commandBuilder, keys, shard, reserved);
			}
		}

		private void InsertConfiguration(bool useUniqueIfPrimaryKeyNotMatch, Table[] tables, SqlCommand command,
			DbCommandBuilder commandBuilder, Dictionary<string, List<UniqueKey>> keys, byte shard, byte reserved)
		{
			var insertBuilder = new StringBuilder();
			insertBuilder.Append(_tableCommandBuilder.InsertCommandPrefix).Append(" VALUES (");

			// add single bigint parameter
			command.Parameters.Add(GetParameterName(TableCommandBuilder.IdColumnName), SqlDbType.BigInt);
			command.Parameters.Add(GetParameterName(TableCommandBuilder.TypeColumnName), SqlDbType.SmallInt);

			foreach (var columnName in TableCommandBuilder.GetColumnNames())
			{
				string parameterName = GetParameterName(columnName);
				insertBuilder.Append(parameterName).Append(",");

				// integer parameters already added
				if (columnName == TableCommandBuilder.IdColumnName || columnName == TableCommandBuilder.TypeColumnName)
					continue;

				command.Parameters.Add(parameterName, SqlDbType.NVarChar, SysNameSize);
			}

			// replace last ",' with ")"
			insertBuilder[insertBuilder.Length - 1] = ')';
			command.CommandText = insertBuilder.ToString();

			foreach (var table in tables)
			{
				string fullTableName = GetFullTableName(commandBuilder, table);

				// TODO: when table.KeyColumnName specified use ColumnsProvider.GetLongColums to find long identifier regardless unique or primary keys
				string targetColumnName = GetTargetColumnNameFromUniqueKeys(fullTableName, table, useUniqueIfPrimaryKeyNotMatch, keys);

				command.Parameters[GetParameterName(TableCommandBuilder.TypeColumnName)].Value = table.TypeId;
				command.Parameters[GetParameterName(TableCommandBuilder.IdColumnName)].Value = 
					(long) new LongId(table.TypeId, shard, reserved, 0);
				command.Parameters[GetParameterName(TableCommandBuilder.TableNameColumnName)].Value = table.TableName;
				command.Parameters[GetParameterName(TableCommandBuilder.KeyColumnName)].Value = targetColumnName;

				command.ExecuteNonQuery();
				// TODO: update identifiers from real tables with storage
			}
		}

		private static string GetParameterName(string columnName)
		{
			return "@" + columnName;
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
			            " sysname null," + TableCommandBuilder.KeyColumnName + " sysname null)");
		}
	}
}