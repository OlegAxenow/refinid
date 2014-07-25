using System;
using System.Data.SqlClient;

namespace RefinId
{
	/// <summary>
	/// Installs <see cref="LongId"/> configuration for "System.Data.SqlClient" provider.
	/// </summary>
	/// <remarks>
	/// Configuration table stores information about last identifiers and types for each configured table.</remarks>
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
		/// Installs necessary configuration into storage.
		/// </summary>
		/// <param name="shard"> Current shard id to be stored into <see cref="LongId.Shard"/>.</param>
		/// <param name="reserved"> Reserved value to be stored into <see cref="LongId.Reserved"/>.</param>
		/// <param name="tables"> Optional tables to be included into configuration.</param>
		public void Install(byte shard, byte reserved, params TableParameters[] tables)
		{
			using (var connection = (SqlConnection)_tableCommandBuilder.OpenConnection())
			{
				var command = connection.CreateCommand();

				RunTableCreation(command);
				if (tables == null) return;

				foreach (var table in tables)
				{
					
				}
			}
			throw new NotImplementedException();
		}

		private void RunTableCreation(SqlCommand command)
		{
			command.Run("IF OBJECT_ID('" + TableName + "') IS NULL " +
			            "CREATE TABLE " + TableName + " (" + TableCommandBuilder.TypeColumnName +
			            " smallint not null primary key, " + TableCommandBuilder.IdColumnName +
			            " bigint not null, " + TableCommandBuilder.TableNameColumnName +
			            " sysname null)");
		}

		/// <summary>
		///     Quoted table name for configuration.
		/// </summary>
		public string TableName
		{
			get { return _tableCommandBuilder.TableName; }
		}
	}
}