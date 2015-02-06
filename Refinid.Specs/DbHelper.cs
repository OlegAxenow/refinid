using System;
using System.Data.Common;
using System.Data.SQLite;

namespace RefinId.Specs
{
	public static class DbHelper
	{
		public const string ConnectionString = "FullUri=file:mydb.db?mode=memory&cache=shared";

		public static DbConnection CreateConnection()
		{
			var connection = new SQLiteConnection(ConnectionString);
			return connection;
		}

		public static int GetTableCount(this DbCommand command, string tableName)
		{
			object count;
			if (command.GetType().Namespace == "System.Data.SQLite")
				count = command.Run("SELECT count(*) FROM sqlite_master WHERE type = 'table' and name = '" + tableName + "'", true);
			else
				count = command.Run("SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + tableName + "'", true);
			return count != null ? Convert.ToInt32(count) : 0;
		}

		public static void DropTableIfExists(this DbConnection connection, string tableName)
		{
			var command = connection.CreateCommand();
			if (GetTableCount(command, tableName) == 1)
				command.Run("DROP TABLE '" + tableName + "'");
		}
	}
}