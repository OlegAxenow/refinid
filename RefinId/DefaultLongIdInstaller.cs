using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using RefinId.Metadata;

namespace RefinId
{
	/// <summary>
	///     Installs <see cref="LongId" /> configuration for ANSI-compliant database provider.
	/// </summary>
	/// <remarks>
	///     Configuration table stores information about last identifiers and types for each configured table.
	///		If your database does compatible with some features used, you can write your own installer.
	/// </remarks>
	public class DefaultLongIdInstaller
	{
		private const string LongDbDataType = "BIGINT";

		private const int SysNameSize = 128;
		private readonly IDbMetadataProvider _metadataProvider;
		private readonly ILongIdStorage _storage;

		/// <summary>
		/// Stores parameters into fields.
		/// </summary>
		/// <param name="metadataProvider"> <see cref="IDbMetadataProvider"/> to retrieve unique keys from storage metedata.</param>
		/// <param name="storage"> <see cref="ILongIdStorage"/> to operate with storage.</param>
		public DefaultLongIdInstaller(IDbMetadataProvider metadataProvider, ILongIdStorage storage)
		{
			if (metadataProvider == null) throw new ArgumentNullException("metadataProvider");
			if (storage == null) throw new ArgumentNullException("storage");

			_metadataProvider = metadataProvider;
			_storage = storage;
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
			var tablesByType = new Dictionary<short, string>();
			foreach (var table in tables)
			{
				if (tablesByType.ContainsKey(table.TypeId))
					throw new ArgumentException(string.Format(
						"Invalid type {0} for the table '{1}' because the table '{2}' already has this type.",
						table.TypeId, table.TableName, tablesByType[table.TypeId]));

				tablesByType.Add(table.TypeId, table.TableName);
			}

			using (var connection = _storage.Builder.OpenConnection())
			{
				var commandBuilder = _storage.Builder.GetDbCommandBuilder();
				DbCommand command = connection.CreateCommand();

				var keys = new Dictionary<string, List<UniqueKey>>();
				foreach (UniqueKey key in _metadataProvider.GetUniqueKeys(command, LongDbDataType))
				{
					if (!useUniqueIfPrimaryKeyNotMatch && !key.IsPrimaryKey) continue;
					
					string fullName = GetFullTableName(commandBuilder, key);
					List<UniqueKey> list;
					if (!keys.TryGetValue(fullName, out list))
						keys.Add(fullName, list = new List<UniqueKey>());
					list.Add(key);
				}

				RunTableCreation(command, commandBuilder);
				
				InsertConfiguration(useUniqueIfPrimaryKeyNotMatch, tables, command, commandBuilder, keys, shard, reserved);
			}
		}

		private void InsertConfiguration(bool useUniqueIfPrimaryKeyNotMatch, Table[] tables, DbCommand command,
			DbCommandBuilder commandBuilder, Dictionary<string, List<UniqueKey>> keys, byte shard, byte reserved)
		{
			var insertBuilder = new StringBuilder();
			insertBuilder.Append(_storage.Builder.InsertCommandPrefix).Append(" VALUES (");

			DbParameter id = null;
			DbParameter type = null;
			DbParameter tableName = null;
			DbParameter key = null;
			DbParameter shardParameter = null;

			foreach (var columnName in TableCommandBuilder.GetColumnNames())
			{
				DbParameter parameter;
				switch (columnName)
				{
					case TableCommandBuilder.IdColumnName:
						parameter = id = AddParameter(command, DbType.Int64, columnName);
						break;
					case TableCommandBuilder.TypeColumnName:
						parameter = type = AddParameter(command, DbType.Int16, columnName);
						break;
					case TableCommandBuilder.TableNameColumnName:
						parameter = tableName = AddParameter(command, DbType.String, columnName, SysNameSize);
						break;
					case TableCommandBuilder.KeyColumnName:
						parameter = key = AddParameter(command, DbType.String, columnName, SysNameSize);
						break;
					case TableCommandBuilder.ShardColumnName:
						parameter = shardParameter = AddParameter(command, DbType.Int16, columnName);
						break;
					default:
						throw new InvalidOperationException(string.Format("Unknown column name '{0}'.", columnName));
				}

				insertBuilder.Append(parameter.ParameterName).Append(",");
			}

			if (id == null || type == null || tableName == null || key == null || shardParameter == null)
				throw new InvalidOperationException("Not all columns obtained from builder.");

			// replace last ",' with ")"
			insertBuilder[insertBuilder.Length - 1] = ')';
			command.CommandText = insertBuilder.ToString();

			foreach (var table in tables)
			{
				string fullTableName = GetFullTableName(commandBuilder, table);

				// TODO: when table.KeyColumnName specified use ColumnsProvider.GetLongColumns to find long identifier regardless unique or primary keys
				string targetColumnName = GetTargetColumnNameFromUniqueKeys(fullTableName, table, useUniqueIfPrimaryKeyNotMatch, keys);

				type.Value = table.TypeId;
				id.Value = (long)new LongId(table.TypeId, shard, reserved);
				tableName.Value = table.TableName;
				key.Value = targetColumnName;
				shardParameter.Value = shard;

				command.ExecuteNonQuery();
			}

			_storage.GetLastValues(true);
		}

		private DbParameter AddParameter(DbCommand command, DbType dbType, string parameterName, int size = 0)
		{
			var parameter = command.CreateParameter();
			parameter.ParameterName = _metadataProvider.GetParameterName(parameterName);
			parameter.DbType = dbType;
			if (size > 0)
				parameter.Size = size;

			command.Parameters.Add(parameter);
			return parameter;
		}

		private static string GetTargetColumnNameFromUniqueKeys(string fullTableName, Table table, bool useUniqueIfPrimaryKeyNotMatch, Dictionary<string, List<UniqueKey>> keys)
		{
			List<UniqueKey> list;
			if (!keys.TryGetValue(fullTableName, out list))
				throw new ArgumentException(string.Format("No key constraint found for {0}.", fullTableName), "table");

			var longSingleColumnKeys = list.Where(x => (x.IsPrimaryKey || useUniqueIfPrimaryKeyNotMatch))
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
			if (string.IsNullOrEmpty(table.Schema))
				return commandBuilder.QuoteIdentifier(table.TableName);
			return commandBuilder.QuoteIdentifier(table.Schema) + "." + commandBuilder.QuoteIdentifier(table.TableName);
		}

		private void RunTableCreation(DbCommand command, DbCommandBuilder commandBuilder)
		{
			if (!_metadataProvider.TableExists(command, _storage.Builder.TableName))
			command.Run("CREATE TABLE " + _storage.Builder.QuotedTableName + " (" +
				commandBuilder.QuoteIdentifier(TableCommandBuilder.TypeColumnName) + " SMALLINT NOT NULL PRIMARY KEY, " + 
				commandBuilder.QuoteIdentifier(TableCommandBuilder.IdColumnName) + " BIGINT NOT NULL, " + 
				commandBuilder.QuoteIdentifier(TableCommandBuilder.TableNameColumnName) + " VARCHAR (" + SysNameSize + ") NULL," + 
				commandBuilder.QuoteIdentifier(TableCommandBuilder.KeyColumnName) + " VARCHAR (" + SysNameSize + ") NULL," +
				commandBuilder.QuoteIdentifier(TableCommandBuilder.ShardColumnName) + " SMALLINT NULL)");
		}
	}
}