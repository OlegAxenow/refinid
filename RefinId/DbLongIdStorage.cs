using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace RefinId
{
	/// <summary>
	///     Uses table in database to store last identifiers.
	/// </summary>
	/// <remarks>By default, expects table _longIds(Id as long).</remarks>
	public class DbLongIdStorage : ILongIdStorage
	{
		private readonly TableCommandBuilder _tableCommandBuilder;

		/// <summary>
		///     Initializes <see cref="_tableCommandBuilder" /> with specified parameters.
		/// </summary>
		/// <param name="connectionString"> See <see cref="TableCommandBuilder" /> for details..</param>
		/// <param name="tableName"> See <see cref="TableCommandBuilder.TableName" /> for details.</param>
		/// <param name="providerName"> See <see cref="TableCommandBuilder" /> for details.</param>
		public DbLongIdStorage(string connectionString, string tableName = null, string providerName = null)
		{
			_tableCommandBuilder = new TableCommandBuilder(connectionString, tableName, providerName);
		}

		/// <summary>
		///     Name of table with information about last identifiers and types.
		/// </summary>
		public string TableName
		{
			get { return _tableCommandBuilder.TableName; }
		}

		/// <summary>
		///     <see cref="ILongIdStorage.GetLastValues" /> implementation.
		/// </summary>
		public List<long> GetLastValues(bool requestFromRealTables = false)
		{
			using (DbConnection connection = _tableCommandBuilder.OpenConnection())
			{
				OnBeforeLoadValues(connection);
				using (DbCommand command = _tableCommandBuilder.CreateSelectCommand(connection))
				{
					if (requestFromRealTables)
						return SelectValuesFromTablesAndSaveToConfiguration(command);
					return SelectValuesFromConfiguration(command);
				}
			}
		}

		private List<long> SelectValuesFromTablesAndSaveToConfiguration(DbCommand command)
		{
			DbCommandBuilder dbCommandBuilder = _tableCommandBuilder.GetDbCommandBuilder();

			throw new NotImplementedException("Reading from real tables with SELECT MAX not implemented.");
		}

		private static List<long> SelectValuesFromConfiguration(DbCommand command)
		{
			var result = new List<long>();
			using (DbDataReader reader = command.ExecuteReader())
			{
				while (reader.Read())
				{
					const int IdOrdinal = 1;
					const int TypeOrdinal = 0;

					var id = (LongId)reader.GetInt64(IdOrdinal);
					if (reader.GetInt16(TypeOrdinal) != id.Type)
						throw new InvalidOperationException(
							string.Format("Type for id {0} should be {1} but equals to {2}.",
								id, id.Type, reader.GetInt16(TypeOrdinal)));

					result.Add(id);
				}
			}

			return result;
		}

		/// <summary>
		///     <see cref="ILongIdStorage.SaveLastValues" /> implementation.
		/// </summary>
		public void SaveLastValues(IEnumerable<long> values,
			bool removeUnusedRows = true)
		{
			if (values == null) throw new ArgumentNullException("values");

			using (DbConnection connection = _tableCommandBuilder.OpenConnection())
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
			throw new NotImplementedException("Should be replaced by optimized code");
			// TODO: use hangfire (optional)
		}

		private void SaveToDatabase(IEnumerable<long> values, DbConnection connection, bool removeUnusedRows)
		{
			// TODO: add/check filter for last value (assert that nobody updates storage without us), may be use SQL (instead of DataSet)
			DbCommandBuilder builder = _tableCommandBuilder.InitializeCommandBuilderAndAdapter(connection);

			var dataSet = new DataSet();
			builder.DataAdapter.Fill(dataSet);
			DataTable table = dataSet.Tables[0];

			PrepareChanges(table, values, removeUnusedRows);

			builder.DataAdapter.InsertCommand = builder.GetInsertCommand();
			builder.DataAdapter.UpdateCommand = builder.GetUpdateCommand();
			builder.DataAdapter.DeleteCommand = builder.GetDeleteCommand();

			builder.DataAdapter.Update(dataSet);
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

				row[TableCommandBuilder.IdColumnName] = id.Value;
			}

			if (removeUnusedRows)
			{
				for (int i = table.Rows.Count - 1; i >= 0; i--)
				{
					DataRow row = table.Rows[i];
					// if "typeid" will be removed, we can use:  ((LongId)Convert.ToInt64(row[IdColumnName])).Type;
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