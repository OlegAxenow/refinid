using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace RefinId
{
	/// <summary>
	/// Uses table in database to store last identifiers.
	/// </summary>
	/// <remarks>By default, expects table _longIds(id as long).</remarks>
	public class DbLongIdStorage : ILongIdStorage
	{
		public const string DefaultTableName = "_longIds";
		public const string DefaultProviderName = "System.Data.SqlClient";
		public const string IdColumnName = "id";
		public const string TypeColumnName = "typeid";

		private readonly string _connectionString;
		private readonly string _tableName;
		private readonly DbProviderFactory _factory;

		/// <summary>
		/// Initializes instance with specified parameters and checks <see cref="DbProviderFactory"/>
		/// creation for <paramref name="providerName"/>.
		/// </summary>
		/// <param name="connectionString"> Valid connection string to access to database.</param>
		/// <param name="providerName"> Provider name to instantiate <see cref="DbProviderFactory"/>.
		/// You can get it from config file (&lt;connectionStrings&gt; element).</param>
		/// <param name="tableName"> Name of the table with last identifiers values.
		/// This table should contains (typeid, id) columns with <see cref="short"/> and <see cref="long"/> types respectively. 
		/// "typeid" column is redundant, but needed because of limited <see cref="DbProviderFactory"/> API.</param>
		/// <param name="removeUnusedRows"> Whether to remove absent entries inside <see cref="SaveLastValues"/>.</param>
		public DbLongIdStorage(string connectionString,
			string providerName = DefaultProviderName,
			string tableName = DefaultTableName,
			bool removeUnusedRows = true)
		{
			if (connectionString == null) throw new ArgumentNullException("connectionString");
			if (providerName == null) throw new ArgumentNullException("providerName");
			if (tableName == null) throw new ArgumentNullException("tableName");

			_connectionString = connectionString;
			RemoveUnusedRows = removeUnusedRows;

			_factory = DbProviderFactories.GetFactory(providerName);
			if (_factory == null)
				throw new ArgumentOutOfRangeException("providerName");

			var dbCommandBuilder = _factory.CreateCommandBuilder();
			if (dbCommandBuilder == null)
				throw new ArgumentOutOfRangeException("providerName");

			_tableName = dbCommandBuilder.QuoteIdentifier(tableName);
		}

		public List<long> GetLastValues()
		{
			var result = new List<long>();
			using (var connection = OpenConnection())
			{
				OnBeforeLoadValues(connection);
				using (var command = connection.CreateCommand())
				{
					command.CommandText = GetSelectCommandText();
					command.CommandType = CommandType.Text;

					using (var reader = command.ExecuteReader())
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

							result.Add(id);
						}
					}
				}
			}

			return result;
		}

		private string GetSelectCommandText()
		{
			return "select " + TypeColumnName + "," + IdColumnName +
			       " from " + _tableName;
		}

		public void SaveLastValues(IEnumerable<long> values)
		{
			if (values == null) throw new ArgumentNullException("values");

			using (var connection = OpenConnection())
			{
				OnBeforeSaveValues(connection);
				using (var command = connection.CreateCommand())
				{
					SaveToDatabase(values, command);
				}
			}
		}

		private void SaveToDatabase(IEnumerable<long> values, DbCommand command)
		{
			var builder = InitializeCommandBuilderAndAdapter(command);

			var dataSet = new DataSet();
			builder.DataAdapter.Fill(dataSet);
			var table = dataSet.Tables[0];

			PrepareChanges(table, values);

			
			builder.DataAdapter.InsertCommand = builder.GetInsertCommand();
			builder.DataAdapter.UpdateCommand = builder.GetUpdateCommand();
			builder.DataAdapter.DeleteCommand = builder.GetDeleteCommand();

			builder.DataAdapter.Update(dataSet);
		}

		private void PrepareChanges(DataTable table, IEnumerable<long> values)
		{
			var tableIdMapByType = CreateMapByType(table);
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
					if (RemoveUnusedRows)
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

			if (RemoveUnusedRows)
			{
				for (int i = table.Rows.Count - 1; i >= 0; i--)
				{
					var row = table.Rows[i];
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

		public bool RemoveUnusedRows { get; private set; }

		private DbCommandBuilder InitializeCommandBuilderAndAdapter(DbCommand command)
		{
			command.CommandType = CommandType.Text;
			command.CommandText = GetSelectCommandText();

			var builder = _factory.CreateCommandBuilder();
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
		/// Allows to append behavior before loading values from database in <see cref="GetLastValues"/>.
		/// </summary>
		/// <param name="connection">Opened connection to database.</param>
		protected virtual void OnBeforeLoadValues(DbConnection connection)
		{
		}

		/// <summary>
		/// Allows to append behavior before saving values to database in <see cref="SaveLastValues"/>.
		/// </summary>
		/// <param name="connection">Opened connection to database.</param>
		protected virtual void OnBeforeSaveValues(DbConnection connection)
		{
		}

		private DbConnection OpenConnection()
		{
			var connection = _factory.CreateConnection();
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

		public string TableName
		{
			get { return _tableName; }
		}
	}
}