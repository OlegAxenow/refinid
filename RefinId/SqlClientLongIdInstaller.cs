using System;

namespace RefinId
{
	/// <summary>
	///     Installer for "System.Data.SqlClient" provider.
	/// </summary>
	public class SqlClientLongIdInstaller
	{
		private readonly string _tableName;

		/// <summary>
		///     Initializes instance with specified parameters.
		/// </summary>
		/// <param name="connectionString"> Valid connection string to access to database.</param>
		/// <param name="tableName"> Name of the table with last identifiers values.</param>
		public SqlClientLongIdInstaller(string connectionString,
			string tableName = DbLongIdStorage.DefaultTableName)
		{
			_tableName = tableName;
		}

		/// <summary>
		/// Installs necessary values into storage.
		/// </summary>
		public void Install()
		{
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