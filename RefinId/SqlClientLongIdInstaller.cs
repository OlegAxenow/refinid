using System;
using System.Data.SqlClient;

namespace RefinId
{
	/// <summary>
	///     Installer for "System.Data.SqlClient" provider.
	/// </summary>
	public class SqlClientLongIdInstaller
	{
		private readonly TableCommandBuilder _tableCommandBuilder;

		/// <summary>
		///     Initializes <see cref="_tableCommandBuilder"/> with specified parameters.
		/// </summary>
		/// <param name="connectionString"> See <see cref="TableCommandBuilder" /> for details..</param>
		/// <param name="tableName"> See <see cref="TableCommandBuilder" /> for details.</param>
		public SqlClientLongIdInstaller(string connectionString, string tableName = null)
		{
			_tableCommandBuilder = new TableCommandBuilder(connectionString, tableName,
				TableCommandBuilder.SqlProviderName);
		}

		/// <summary>
		/// Installs necessary values into storage.
		/// </summary>
		public void Install()
		{
			using (var connection = (SqlConnection)_tableCommandBuilder.OpenConnection())
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
			get { return _tableCommandBuilder.TableName; }
		}
	}
}