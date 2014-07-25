using System;
using System.Data.SqlClient;
using NUnit.Framework;
using RefinId;

namespace Refinid.Specs
{
	[TestFixture]
	public class SqlClientLongIdInstallerSpec
	{
		[Test]
		public void Table_should_be_created()
		{
			// arrange
			var installer = new SqlClientLongIdInstaller(ConnectionHelper.ConnectionString);
			using (SqlConnection connection = ConnectionHelper.CreateConnection())
			{
				SqlCommand command = connection.CreateCommand();
				command.Run("IF OBJECT_ID('" + installer.TableName + "') IS NOT NULL " +
							" DROP TABLE " + installer.TableName);

				// act
				installer.Install(0, 0, null);

				// assert
				command.CommandText = "SELECT OBJECT_ID('" + installer.TableName + "')";
				var result = command.ExecuteScalar();
				Assert.That(result, Is.Not.Null);
				Assert.That(Convert.ToInt64(result), Is.GreaterThan(0));
			}
		}
	}
}