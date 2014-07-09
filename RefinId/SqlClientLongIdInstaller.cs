using System;
using System.Data.SqlClient;

namespace RefinId
{
	/// <summary>
	///     Installer for "System.Data.SqlClient" provider.
	/// </summary>
	public class SqlClientLongIdInstaller
	{
		private readonly string _connectionString;
		private readonly string _tableName;

		/// <summary>
		///     Initializes instance with specified parameters.
		/// </summary>
		/// <param name="connectionString"> Valid connection string to access to database.</param>
		/// <param name="tableName"> Name of the table with last identifiers values.</param>
		public SqlClientLongIdInstaller(string connectionString,
			string tableName = TableCommandBuilder.DefaultTableName)
		{
			if (connectionString == null) throw new ArgumentNullException("connectionString");
			if (tableName == null) throw new ArgumentNullException("tableName");

			_connectionString = connectionString;
			_tableName = tableName;
		}

		/// <summary>
		/// Installs necessary values into storage.
		/// </summary>
		public void Install()
		{
			using (var connection = new SqlConnection(_connectionString))
			{
				var command = connection.CreateCommand();

				command.Run("IF OBJECT_ID('" + TableName + "') IS NULL" + 
							"CREATE TABLE " + TableName + " (" + TableCommandBuilder.TypeColumnName +
							" smallint not null primary key, " + TableCommandBuilder.IdColumnName +
							" bigint not null, " + TableCommandBuilder.TableNameColumnName +
							" sysname null)");
			}
			throw new NotImplementedException();
		}

		/// <summary>
		///     Name of the table with information about last identifiers and types.
		/// </summary>
		public string TableName
		{
			get { return _tableName; }
		}
	}
}