using System;
using System.Data.Common;

namespace RefinId
{
	/// <summary>
	/// Append "installer" behavior to <see cref="DbLongIdStorage"/>.
	/// </summary>
	public class DbLongIdStorageWithInstaller : DbLongIdStorage
	{
		/// <summary>
		/// Initializes instance with specified parameters and checks <see cref="DbProviderFactory"/>
		/// creation for <paramref name="providerName"/>.
		/// </summary>
		/// <param name="connectionString">Valid connection string to access to database.</param>
		/// <param name="providerName">Provider name to instantiate <see cref="DbProviderFactory"/>.</param>
		/// <param name="tableName">Name of the table with last identifiers values.</param>
		public DbLongIdStorageWithInstaller(string connectionString, 
			string providerName = "System.Data.SqlClient",
			string tableName = "_longIds") : base(connectionString, providerName, tableName)
		{
		}

		/// <summary>
		/// This implementaion tries to create table <see name="DbLongIdStorage.TableName"/> if not exists
		/// </summary>
		/// <param name="connection">Opened connection to database.</param>
		protected override void OnBeforeLoadValues(DbConnection connection)
		{
			base.OnBeforeLoadValues(connection);

			throw new NotImplementedException();
		}
	}
}