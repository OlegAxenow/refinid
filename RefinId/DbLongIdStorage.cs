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
		private readonly string _connectionString;
		private readonly string _providerName;
		private readonly string _tableName;
		private readonly DbProviderFactory _factory;

		/// <summary>
		/// Initializes instance with specified parameters and checks <see cref="DbProviderFactory"/>
		/// creation for <paramref name="providerName"/>.
		/// </summary>
		/// <param name="connectionString">Valid connection string to access to database.</param>
		/// <param name="providerName">Provider name to instantiate <see cref="DbProviderFactory"/>.
		/// You can get it from config file (&lt;connectionStrings&gt; element).</param>
		/// <param name="tableName">Name of the table with last identifiers values.</param>
		public DbLongIdStorage(string connectionString,
			string providerName = "System.Data.SqlClient",
			string tableName = "_longIds")
		{
			if (connectionString == null) throw new ArgumentNullException("connectionString");
			if (providerName == null) throw new ArgumentNullException("providerName");
			if (tableName == null) throw new ArgumentNullException("tableName");

			_connectionString = connectionString;
			_providerName = providerName;

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
					command.CommandText = "select id from " + _tableName;
					command.CommandType = CommandType.Text;

					using (var reader = command.ExecuteReader())
					{
						while (reader.Read())
							result.Add(reader.GetInt64(0));
					}
				}
			}

			return result;
		}

		public void SaveLastValues(IEnumerable<long> values)
		{
			using (var connection = OpenConnection())
			{
				OnBeforeSaveValues(connection);
				using (var command = connection.CreateCommand())
				{
					command.CommandType = CommandType.Text;

					command.CommandText = "DELETE FROM " + _tableName;
					command.ExecuteNonQuery();

					// TODO: may be use DataSet + DataAdapter as more universal approach (no hard-coded provider-specific parameter placeholders etc.)

					var parameter = command.CreateParameter();
					command.Parameters.Add(parameter);
					
					command.CommandText = "INSERT INTO " + _tableName + "(id) VALUES("
						+ GetParameterPlaceholder(parameter, "id") + ")";

					command.Prepare();
					foreach (long value in values)
					{
						parameter.Value = value;
						command.ExecuteNonQuery();
					}
				}
			}

			throw new NotImplementedException();
		}

		private string GetParameterPlaceholder(DbParameter parameter, string pureName)
		{
			string placeholder;

			switch (_providerName)
			{
				case "System.Data.SqlClient":
					placeholder ="@" + pureName;
					parameter.ParameterName = placeholder;
					break;
				default:
					throw new NotSupportedException(
						string.Format("{0} database provider does not supported.", _providerName));
			}
			throw new NotImplementedException();
			
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