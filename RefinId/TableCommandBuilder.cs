using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;

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
		public const string IdColumnName = "Id";

		/// <summary>
		///     Name of the type column in <see cref="DbLongIdStorage.TableName" />.
		/// </summary>
		public const string TypeColumnName = "TypeId";

		/// <summary>
		///     Name of the column with table name for type,
		///     specified by <see cref="TypeColumnName" /> in <see cref="DbLongIdStorage.TableName" />.
		/// </summary>
		public const string TableNameColumnName = "TableName";

		/// <summary>
		///     Name of the column with key column name from referenced table in <see cref="DbLongIdStorage.TableName" />.
		/// </summary>
		public const string KeyColumnName = "KeyName";

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
		private readonly string _insertCommandPrefix;
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
		///     This table should contains (TypeId not null, Id not null, TableName null)
		///     columns with <see cref="short" />,
		///     <see cref="long" /> and <see cref="string" /> types respectively.
		///     "TypeId" column is redundant, but needed because of limited <see cref="DbProviderFactory" /> API.
		/// </param>
		/// <param name="providerName">
		///     Provider name to instantiate <see cref="DbProviderFactory" />.
		///     <see cref="DefaultProviderName" /> if not specified.
		///     You can get it from config file (&lt;connectionStrings&gt; element).
		/// </param>
		/// <exception cref="InvalidOperationException">
		///     If <see cref="DbProviderFactory" /> for <paramref name="providerName" />
		///     cannot be obtained or cannot instantiate necessary classes.
		/// </exception>
		public TableCommandBuilder(string connectionString, string tableName = null, string providerName = null)
		{
			if (providerName == null) providerName = DefaultProviderName;
			if (tableName == null) tableName = DefaultTableName;
			if (connectionString == null) throw new ArgumentNullException("connectionString");

			_connectionString = connectionString;

			_factory = DbProviderFactories.GetFactory(providerName);
			if (_factory == null)
				throw new InvalidOperationException("providerName");

			_tableName = GetDbCommandBuilder().QuoteIdentifier(tableName);

			var stringBuilder = new StringBuilder();
			stringBuilder.Append("select ");
			AppendColumnList(stringBuilder);
			stringBuilder.Append(" from ").Append(_tableName);
			_selectCommandText = stringBuilder.ToString();

			stringBuilder.Length = 0;
			stringBuilder.Append("insert into ").Append(_tableName).Append(" (");
			AppendColumnList(stringBuilder);
			stringBuilder.Append(") ");
			_insertCommandPrefix = stringBuilder.ToString();
		}

		/// <summary>
		///     Name of the table with information about last identifiers and types.
		/// </summary>
		public string TableName
		{
			get { return _tableName; }
		}

		/// <summary>
		///     Returns first command part for insert before VALUES, i.e. INSERT INTO Table1 (...).
		/// </summary>
		public string InsertCommandPrefix
		{
			get { return _insertCommandPrefix; }
		}

		private static void AppendColumnList(StringBuilder stringBuilder)
		{
			foreach (string columnName in GetColumnNames())
				stringBuilder.Append(columnName).Append(",");
			stringBuilder.Length--;
		}

		/// <summary>
		///     Returns <see cref="DbCommandBuilder" /> from factory.
		/// </summary>
		public DbCommandBuilder GetDbCommandBuilder()
		{
			DbCommandBuilder dbCommandBuilder = _factory.CreateCommandBuilder();
			if (dbCommandBuilder == null)
				throw new InvalidOperationException("providerName");
			return dbCommandBuilder;
		}

		/// <summary>
		///     Creates and initializes <see cref="DbCommand" /> for select statement from <see cref="TableName" />.
		/// </summary>
		public DbCommand CreateSelectCommand(DbConnection connection)
		{
			DbCommand command = connection.CreateCommand();
			command.CommandText = _selectCommandText;
			command.CommandType = CommandType.Text;
			return command;
		}

		/// <summary>
		///     Initializes <see cref="DbCommandBuilder" /> with specified command and <see cref="CreateSelectCommand" />.
		/// </summary>
		/// <exception cref="InvalidOperationException"> If a <see cref="DbProviderFactory" /> cannot create needed classes. </exception>
		public DbCommandBuilder InitializeCommandBuilderAndAdapter(DbConnection connection)
		{
			var command = CreateSelectCommand(connection);

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

		/// <summary>
		///     Returns all column names for configuration table.
		/// </summary>
		public static IEnumerable<string> GetColumnNames()
		{
			yield return TypeColumnName;
			yield return IdColumnName;
			yield return TableNameColumnName;
			yield return KeyColumnName;
		}
	}
}