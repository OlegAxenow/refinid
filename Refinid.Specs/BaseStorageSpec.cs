using System.Data.Common;
using NUnit.Framework;
using RefinId;

namespace Refinid.Specs
{
	public class BaseStorageSpec
	{
		protected const string DbProviderName = "System.Data.SQLite";
		protected const string TableName = TableCommandBuilder.DefaultTableName;

		[TestFixtureSetUp]
		public void TestFixtureSetUp()
		{
			using (DbConnection connection = DbHelper.CreateConnection())
			{
				var command = connection.CreateCommand();

				// create table to avoid problems with test metadata providers
				connection.DropTableIfExists(TableName);
				command.Run("CREATE TABLE " + TableName + " (" + TableCommandBuilder.TypeColumnName +
							" SMALLINT NOT NULL PRIMARY KEY, " + TableCommandBuilder.IdColumnName +
							" BIGINT NOT NULL, " + TableCommandBuilder.TableNameColumnName +
							" VARCHAR(128) NULL," + TableCommandBuilder.KeyColumnName + " VARCHAR(128) NULL)");
			}
		} 
	}
}