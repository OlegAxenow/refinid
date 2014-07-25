using System;
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
			using (var connection = ConnectionHelper.CreateConnection())
			{
				var command = connection.CreateCommand();
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

		[Test]
		public void Specified_tables_should_be_appended_to_configuration()
		{
			// arrange
			var installer = new SqlClientLongIdInstaller(ConnectionHelper.ConnectionString);
			using (var connection = ConnectionHelper.CreateConnection())
			{
				var command = connection.CreateCommand();
				command.Run("IF OBJECT_ID('TestId1') IS NOT NULL DROP TABLE TestId1; " +
							" CREATE TABLE TestId1 (Id bigint PRIMARY KEY, Name sysname);");
				command.Run("IF OBJECT_ID('TestId2') IS NOT NULL DROP TABLE TestId2; " +
							" CREATE TABLE TestId2 (TestId2Id bigint PRIMARY KEY, Name sysname);");
				command.Run("INSERT TestId1 VALUES(123, 'Test')");
				
				// act
				installer.Install(0, 0, new TableParameters(0, "TestId1"), new TableParameters(1, "TestId2"));

				// assert
				command.CommandText = "SELECT * FROM " + installer.TableName + " ORDER BY " + TableCommandBuilder.IdColumnName;
				using (var reader = command.ExecuteReader())
				{
					int idIndex = reader.GetOrdinal(TableCommandBuilder.IdColumnName);

					Assert.That(reader.Read());
					Assert.That(reader.GetInt64(idIndex), Is.EqualTo(123));

					Assert.That(reader.Read());
					Assert.That(reader.GetInt64(idIndex), Is.EqualTo(new LongId(1, 0, 0, 0)));

					Assert.That(!reader.Read());
				}
			}
		}

		[Test]
		public void Tables_without_primary_keys_should_cause_errors()
		{
			// arrange
			var installer = new SqlClientLongIdInstaller(ConnectionHelper.ConnectionString);
			using (var connection = ConnectionHelper.CreateConnection())
			{
				var command = connection.CreateCommand();
				command.Run("IF OBJECT_ID('TestId1') IS NOT NULL DROP TABLE TestId1; " +
							" CREATE TABLE TestId1 (Id bigint, Name sysname);");

				// act + assert
				
				Assert.That(() => installer.Install(0, 0, new TableParameters(0, "TestId1")), Throws.ArgumentException);
			}
		}

		[Test]
		public void Tables_with_not_bigint_primary_keys_should_cause_errors()
		{
			// arrange
			var installer = new SqlClientLongIdInstaller(ConnectionHelper.ConnectionString);
			using (var connection = ConnectionHelper.CreateConnection())
			{
				var command = connection.CreateCommand();
				command.Run("IF OBJECT_ID('TestId1') IS NOT NULL DROP TABLE TestId1; " +
							" CREATE TABLE TestId1 (Id int PRIMARY KEY, Name sysname);");

				// act + assert

				Assert.That(() => installer.Install(0, 0, new TableParameters(0, "TestId1")), Throws.ArgumentException);
			}
		}
	}
}