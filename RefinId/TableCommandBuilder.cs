using System;
using System.Data;
using System.Data.Common;

namespace RefinId
{
	/// <summary>
	///     Provides methods to build commands for table with <see cref="LongId" />s.
	/// </summary>
	public class TableCommandBuilder
	{
		/// <summary>
		///     Name of the identifier column in <see cref="DbLongIdStorage.TableName" />.
		/// </summary>
		public const string IdColumnName = "id";

		/// <summary>
		///     Name of the type column in <see cref="DbLongIdStorage.TableName" />.
		/// </summary>
		public const string TypeColumnName = "typeid";

		/// <summary>
		///     Name of the column with table name for type,
		///     specified by <see cref="TypeColumnName" /> in <see cref="DbLongIdStorage.TableName" />.
		/// </summary>
		public const string TableNameColumnName = "tablename";

		/// <summary>
		///     Default name of the table with information about last identifiers and types.
		/// </summary>
		public const string DefaultTableName = "_longIds";

		/// <summary>
		///     Default provider's invariant name.
		/// </summary>
		public const string DefaultProviderName = SqlProviderName;

		/// <summary>
		///     MS SQL provider's invariant name.
		/// </summary>
		public const string SqlProviderName = "System.Data.SqlClient";

		private readonly string _connectionString;
		private readonly DbProviderFactory _factory;
		private readonly string _selectCommandText;
		private readonly string _tableName;

		/// <summary>
		///     Initializes instance with specified parameters and checks <see cref="DbProviderFactory" />
		///     creation for <paramref name="providerName" />.
		/// </summary>
		/// <param name="connectionString"> Valid connection string to access a database.</param>
		/// <param name="tableName">
		///     Name of the table with information about last identifiers and types.
		///     <see cref="DefaultTableName" /> if not specified.
		///     This table should contains (typeid not null, id not null, tablename null)
		///     columns with <see cref="short" />,
		///     <see cref="long" /> and <see cref="string" /> types respectively.
		///     "typeid" column is redundant, but needed because of limited <see cref="DbProviderFactory" /> API.
		/// </param>
		/// <param name="providerName">
		///     Provider name to instantiate <see cref="DbProviderFactory" />.
		///     <see cref="DefaultProviderName" /> if not specified.
		///     You can get it from config file (&lt;connectionStrings&gt; element).
		/// </param>
		public TableCommandBuilder(string connectionString, string tableName = null, string providerName = null)
		{
			if (providerName == null) providerName = DefaultProviderName;
			if (tableName == null) tableName = DefaultTableName;
			if (connectionString == null) throw new ArgumentNullException("connectionString");

			_connectionString = connectionString;

			_factory = DbProviderFactories.GetFactory(providerName);
			if (_factory == null)
				throw new ArgumentOutOfRangeException("providerName");

			DbCommandBuilder dbCommandBuilder = _factory.CreateCommandBuilder();
			if (dbCommandBuilder == null)
				throw new ArgumentOutOfRangeException("providerName");

			_tableName = dbCommandBuilder.QuoteIdentifier(tableName);

			_selectCommandText = "select " + TypeColumnName + "," + IdColumnName + "," + TableNameColumnName +
			                     " from " + _tableName;
		}

		/// <summary>
		///     Name of the table with information about last identifiers and types.
		/// </summary>
		public string TableName
		{
			get { return _tableName; }
		}

		/// <summary>
		///     Command text for select statement from <see cref="TableName" />.
		/// </summary>
		public string SelectCommandText
		{
			get { return _selectCommandText; }
		}

		/// <summary>
		///     Initializes <see cref="DbCommandBuilder" /> with specified command and <see cref="SelectCommandText" />.
		/// </summary>
		/// <exception cref="InvalidOperationException"> If a <see cref="DbProviderFactory" /> cannot create needed classes. </exception>
		public DbCommandBuilder InitializeCommandBuilderAndAdapter(DbCommand command)
		{
			command.CommandType = CommandType.Text;
			command.CommandText = _selectCommandText;

			DbCommandBuilder builder = _factory.CreateCommandBuilder();
			if (builder == null)
				throw new InvalidOperationException(
					String.Format("Factory {0} does not support command builders.", _factory.GetType().Name));

			builder.DataAdapter = _factory.CreateDataAdapter();
			if (builder.DataAdapter == null)
				throw new InvalidOperationException(
					String.Format("Factory {0} does not support data adapters.", _factory.GetType().Name));

			builder.DataAdapter.SelectCommand = command;
			return builder;
		}

		/// <summary>
		///     Creates and opens new <see cref="DbConnection" /> instance.
		/// </summary>
		/// <exception cref="InvalidOperationException"> If <see cref="DbProviderFactory" /> returns null connection.</exception>
		public DbConnection OpenConnection()
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