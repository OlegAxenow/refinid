using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace RefinId
{
	/// <summary>
	///     Uses table in database to store last identifiers.
	/// </summary>
	/// <remarks>By default, expects table _longIds(id as long).</remarks>
	public class DbLongIdStorage : ILongIdStorage
	{
		/// <summary>
		///     Default name of the table with information about last identifiers and types.
		/// </summary>
		public const string DefaultTableName = "_longIds";

		/// <summary>
		///     Default provider's invariant name.
		/// </summary>
		public const string DefaultProviderName = "System.Data.SqlClient";

		/// <summary>
		///     Name of the identifier column in <see cref="TableName" />.
		/// </summary>
		public const string IdColumnName = "id";

		/// <summary>
		///     Name of the type column in <see cref="TableName" />.
		/// </summary>
		public const string TypeColumnName = "typeid";

		/// <summary>
		///     Name of the column with table name for type,
		///     specified by <see cref="TypeColumnName" /> in <see cref="TableName" />.
		/// </summary>
		public const string TableNameColumnName = "tablename";

		private readonly string _connectionString;
		private readonly DbProviderFactory _factory;
		private readonly string _tableName;

		/// <summary>
		///     Initializes instance with specified parameters and checks <see cref="DbProviderFactory" />
		///     creation for <paramref name="providerName" />.
		/// </summary>
		/// <param name="connectionString"> Valid connection string to access to database.</param>
		/// <param name="providerName">
		///     Provider name to instantiate <see cref="DbProviderFactory" />.
		///     You can get it from config file (&lt;connectionStrings&gt; element).
		/// </param>
		/// <param name="tableName">
		///     Name of the table with information about last identifiers and types.
		///     This table should contains (typeid, id) columns with <see cref="short" /> and <see cref="long" /> types
		///     respectively.
		///     "typeid" column is redundant, but needed because of limited <see cref="DbProviderFactory" /> API.
		/// </param>
		public DbLongIdStorage(string connectionString,
			string providerName = DefaultProviderName,
			string tableName = DefaultTableName)
		{
			if (connectionString == null) throw new ArgumentNullException("connectionString");
			if (providerName == null) throw new ArgumentNullException("providerName");
			if (tableName == null) throw new ArgumentNullException("tableName");

			_connectionString = connectionString;

			_factory = DbProviderFactories.GetFactory(providerName);
			if (_factory == null)
				throw new ArgumentOutOfRangeException("providerName");

			DbCommandBuilder dbCommandBuilder = _factory.CreateCommandBuilder();
			if (dbCommandBuilder == null)
				throw new ArgumentOutOfRangeException("providerName");

			_tableName = dbCommandBuilder.QuoteIdentifier(tableName);
		}

		/// <summary>
		///     Name of table with information about last identifiers and types.
		/// </summary>
		public string TableName
		{
			get { return _tableName; }
		}

		/// <summary>
		///     <see cref="ILongIdStorage.GetLastValues" /> implementation.
		/// </summary>
		public List<long> GetLastValues(bool requestFromRealTables = false)
		{
			var result = new List<long>();
			using (DbConnection connection = OpenConnection())
			{
				OnBeforeLoadValues(connection);
				using (DbCommand command = connection.CreateCommand())
				{
					command.CommandText = GetSelectCommandText();
					command.CommandType = CommandType.Text;

					using (DbDataReader reader = command.ExecuteReader())
					{
						while (reader.Read())
						{
							const int idOrdinal = 1;
							const int typeOrdinal = 0;

							var id = (LongId)reader.GetInt64(idOrdinal);
							if (reader.GetInt16(typeOrdinal) != id.Type)
								throw new InvalidOperationException(
									string.Format("Type for id {0} should be {1} but equals to {2}.",
										id, id.Type, reader.GetInt16(typeOrdinal)));

							if (!requestFromRealTables)
							{
								result.Add(id);
							}

							// TODO: implement reading from real tables with SELECT MAX(...
						}
					}
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

			using (DbConnection connection = OpenConnection())
			{
				OnBeforeSaveValues(connection);
				using (DbCommand command = connection.CreateCommand())
				{
					SaveToDatabase(values, command, removeUnusedRows);
				}
			}
		}

		/// <summary>
		///     <see cref="ILongIdStorage.SaveLastValue" /> implementation.
		/// </summary>
		public void SaveLastValue(long value)
		{
			// TODO: replace by optimized code
			SaveLastValues(new[] { value }, false);
			throw new NotImplementedException();
		}

		private string GetSelectCommandText()
		{
			return "select " + TypeColumnName + "," + IdColumnName + "," + TableNameColumnName +
			       " from " + _tableName;
		}

		private void SaveToDatabase(IEnumerable<long> values, DbCommand command, bool removeUnusedRows)
		{
			DbCommandBuilder builder = InitializeCommandBuilderAndAdapter(command);

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
					row[TypeColumnName] = id.Type;
				}
				row[IdColumnName] = id.Value;
			}

			if (removeUnusedRows)
			{
				for (int i = table.Rows.Count - 1; i >= 0; i--)
				{
					DataRow row = table.Rows[i];
					// if "typeid" will be removed, we can use:  ((LongId)Convert.ToInt64(row[IdColumnName])).Type;
					if (tableIdMapByType.ContainsKey(Convert.ToInt16(row[TypeColumnName])))
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
				LongId id = Convert.ToInt64(row[IdColumnName]);
				tableIdMapByType.Add(id.Type, row);
			}
			return tableIdMapByType;
		}

		private DbCommandBuilder InitializeCommandBuilderAndAdapter(DbCommand command)
		{
			command.CommandType = CommandType.Text;
			command.CommandText = GetSelectCommandText();

			DbCommandBuilder builder = _factory.CreateCommandBuilder();
			if (builder == null)
				throw new InvalidOperationException(
					string.Format("Factory {0} does not support command builders.", _factory.GetType().Name));

			builder.DataAdapter = _factory.CreateDataAdapter();
			if (builder.DataAdapter == null)
				throw new InvalidOperationException(
					string.Format("Factory {0} does not support data adapters.", _factory.GetType().Name));

			builder.DataAdapter.SelectCommand = command;
			return builder;
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

		private DbConnection OpenConnection()
		{
			DbConnection connection = _factory.CreateConnection();
			if (connection == null) throw new InvalidOperationException("Cannot create connection to database.");
			try
			{
				connection.ConnectionString = _connectionString;
				connection.Open();
			}
			catch
			{
				try
				{
					connection.Close();
				} // ReSharper disable once EmptyGeneralCatchClause
				catch
				{
				}
				throw;
			}
			return connection;
		}
	}
}