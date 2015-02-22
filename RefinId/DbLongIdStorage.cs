using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;

namespace RefinId
{
	/// <summary>
	///     Uses table in database to store last identifiers.
	/// </summary>
	/// <remarks>By default, expects table _longIds(Id as long).</remarks>
	public class DbLongIdStorage : ILongIdStorage
	{
		private readonly TableCommandBuilder _builder;

		/// <summary>
		///     Initializes <see cref="_builder" /> with specified parameters.
		/// </summary>
		/// <param name="connectionString"> See <see cref="Builder" /> for details.</param>
		/// <param name="dbProviderName"> See <see cref="Builder" /> constructor for details.</param>
		/// <param name="tableName"> See <see cref="RefinId.TableCommandBuilder.QuotedTableName" /> for details.</param>
		public DbLongIdStorage(string connectionString, string dbProviderName, string tableName = null)
		{
			_builder = new TableCommandBuilder(connectionString, dbProviderName, tableName);
		}

		/// <summary>
		///     <see cref="ILongIdStorage.Builder" /> implementation.
		/// </summary>
		public TableCommandBuilder Builder
		{
			get { return _builder; }
		}

		/// <summary>
		///     <see cref="ILongIdStorage.GetLastValues" /> implementation.
		/// </summary>
		public List<long> GetLastValues(bool requestFromRealTables = false)
		{
			using (DbConnection connection = _builder.OpenConnection())
			{
				OnBeforeLoadValues(connection);

				if (requestFromRealTables)
					return SaveToDatabase(null, connection, false);
				return SelectValuesFromConfiguration(connection);
			}
		}

		private List<long> SelectValuesFromConfiguration(DbConnection connection)
		{
			var result = new List<long>();
			using (var command = _builder.CreateSelectCommand(connection))
			{
				using (var reader = command.ExecuteReader())
				{
					int idOrdinal = reader.GetOrdinal(TableCommandBuilder.IdColumnName);
					int typeOrdinal = reader.GetOrdinal(TableCommandBuilder.TypeColumnName);

					while (reader.Read())
					{
						var id = (LongId)reader.GetInt64(idOrdinal);
						if (reader.GetInt16(typeOrdinal) != id.Type)
							throw new InvalidOperationException(
								string.Format("Type for id {0} should be {1} but equals to {2}.",
									id, id.Type, reader.GetInt16(typeOrdinal)));

						result.Add(id);
					}
				}
			}

			return result;
		}

		/// <summary>
		///     <see cref="ILongIdStorage.SaveLastValues" /> implementation.
		/// </summary>
		public void SaveLastValues(IEnumerable<long> values, bool removeUnusedRows = true)
		{
			if (values == null) throw new ArgumentNullException("values");

			using (DbConnection connection = _builder.OpenConnection())
			{
				OnBeforeSaveValues(connection);
				SaveToDatabase(values, connection, removeUnusedRows);
			}
		}

		/// <summary>
		///     <see cref="ILongIdStorage.SaveLastValue" /> implementation.
		/// </summary>
		public void SaveLastValue(long value)
		{
			SaveLastValues(new[] { value }, false);
			// TODO: should be replaced by optimized code
			// TODO: use hangfire (optional)
		}

		private List<long> SaveToDatabase(IEnumerable<long> values, DbConnection connection, bool removeUnusedRows)
		{
			// TODO: add/check filter for last value (assert that nobody updates storage without us), may be use SQL (instead of DataSet)
			DbCommandBuilder commandBuilder = _builder.InitializeCommandBuilderAndAdapter(connection);

			var dataSet = new DataSet();
			commandBuilder.DataAdapter.Fill(dataSet);
			DataTable table = dataSet.Tables[0];
			List<long> result;

			if (values == null)
			{
				result = LoadMaxIdentifiersFromDb(connection, table, commandBuilder);
			}
			else
			{
				result = new List<long>(values);
			}

			PrepareChanges(table, result, removeUnusedRows);

			commandBuilder.DataAdapter.InsertCommand = commandBuilder.GetInsertCommand();
			commandBuilder.DataAdapter.UpdateCommand = commandBuilder.GetUpdateCommand();
			commandBuilder.DataAdapter.DeleteCommand = commandBuilder.GetDeleteCommand();

			commandBuilder.DataAdapter.Update(dataSet);

			return result;
		}

		private static List<long> LoadMaxIdentifiersFromDb(DbConnection connection, DataTable table, DbCommandBuilder commandBuilder)
		{
			var dbValues = new List<long>();

			int tableNameIndex = table.Columns.IndexOf(TableCommandBuilder.TableNameColumnName);
			int keyNameIndex = table.Columns.IndexOf(TableCommandBuilder.KeyColumnName);
			int typeIndex = table.Columns.IndexOf(TableCommandBuilder.TypeColumnName);
			int shardIndex = table.Columns.IndexOf(TableCommandBuilder.ShardColumnName);

			foreach (DataRow row in table.Rows)
			{
				string tableName = row[tableNameIndex].ToString();
				string keyName = row[keyNameIndex].ToString();
				var type = Convert.ToInt16(row[typeIndex]);
				var shard = Convert.ToByte(row[shardIndex]);

				var commandText = new StringBuilder();
				commandText.Append("SELECT MAX(").Append(commandBuilder.QuoteIdentifier(keyName)).Append(")")
					.Append(" FROM ").Append(commandBuilder.QuoteIdentifier(tableName));
				using (var command = connection.CreateCommand())
				{
					command.CommandText = commandText.ToString();
					var lastValueInTable = command.ExecuteScalar();
					if (lastValueInTable == null || lastValueInTable == DBNull.Value)
					{
						dbValues.Add(new LongId(type, shard, 0));
					}
					else
					{
						LongId id = Convert.ToInt64(lastValueInTable);
						if (id.Type != type) throw new InvalidOperationException(
							string.Format("Table {0} has last id with type {1} instead of {2} (as configured).", tableName, id.Type, type));
						dbValues.Add(id);
					}
				}
			}
			return dbValues;
		}

		private void PrepareChanges(DataTable table, IEnumerable<long> values, bool removeUnusedRows)
		{
			Dictionary<short, DataRow> tableIdMapByType = CreateMapByType(table);
			// HashSet to detect non-unique values
			var uniqueValueTypes = new HashSet<short>();

			foreach (LongId id in values)
			{
				if (!uniqueValueTypes.Add(id.Type))
				{
					throw new ArgumentException(
						string.Format("Duplicated type {0} for id {1}.", id.Type, id),
						"values");
				}

				DataRow row;
				if (tableIdMapByType.TryGetValue(id.Type, out row))
				{
					if (removeUnusedRows)
					{
						// remove map entry to detect deleted rows
						tableIdMapByType.Remove(id.Type);
					}
				}
				else
				{
					row = table.NewRow();
					table.Rows.Add(row);
					row[TableCommandBuilder.TypeColumnName] = id.Type;
				}

				row[TableCommandBuilder.IdColumnName] = id.Data;
			}

			if (removeUnusedRows)
			{
				for (int i = table.Rows.Count - 1; i >= 0; i--)
				{
					DataRow row = table.Rows[i];
					// TODO: if "typeid" will be removed, we can use:  ((LongId)Convert.ToInt64(row[IdColumnName])).Type;
					if (tableIdMapByType.ContainsKey(Convert.ToInt16(row[TableCommandBuilder.TypeColumnName])))
					{
						row.Delete();
					}
				}
			}
		}

		private static Dictionary<short, DataRow> CreateMapByType(DataTable table)
		{
			var tableIdMapByType = new Dictionary<short, DataRow>(table.Rows.Count);
			foreach (DataRow row in table.Rows)
			{
				LongId id = Convert.ToInt64(row[TableCommandBuilder.IdColumnName]);
				tableIdMapByType.Add(id.Type, row);
			}

			return tableIdMapByType;
		}

		/// <summary>
		///     Allows to append behavior before loading values from database in <see cref="GetLastValues" />.
		/// </summary>
		/// <param name="connection">Opened connection to database.</param>
		protected virtual void OnBeforeLoadValues(DbConnection connection)
		{
		}

		/// <summary>
		///     Allows to append behavior before saving values to database in <see cref="SaveLastValues" />.
		/// </summary>
		/// <param name="connection">Opened connection to database.</param>
		protected virtual void OnBeforeSaveValues(DbConnection connection)
		{
		}
	}
}