using System;
using NUnit.Framework;
using RefinId;
using RefinId.Metadata;

namespace Refinid.Specs
{
	[TestFixture]
	public class DefaultLongIdInstallerSpec : BaseStorageSpec
	{
		[Test]
		public void Table_should_be_created()
		{
			// arrange
			var installer = new DefaultLongIdInstaller(DbHelper.ConnectionString, new SQLiteDbMetadataProvider(), DbProviderName);
			using (var connection = DbHelper.CreateConnection())
			{
				connection.DropTableIfExists(TableName);
				var command = connection.CreateCommand();
				
				// act
				installer.Install(0, 0, false, null);

				// assert
				command.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' and name = '" + TableName + "'";
				var result = command.ExecuteScalar();
				Assert.That(result, Is.Not.Null);
				Assert.That(Convert.ToInt64(result), Is.EqualTo(1));
			}
		}

		[Test]
		public void Specified_tables_should_be_appended_to_configuration()
		{
			// arrange
			var installer = new DefaultLongIdInstaller(DbHelper.ConnectionString, new SQLiteDbMetadataProvider(), DbProviderName);
			using (var connection = DbHelper.CreateConnection())
			{
				var command = connection.CreateCommand();

				connection.DropTableIfExists(TableName);
				connection.DropTableIfExists("TestId1");
				connection.DropTableIfExists("TestId2");
				command.Run("CREATE TABLE TestId1 (Id BIGINT PRIMARY KEY, Name VARCHAR(128));");
				command.Run("CREATE TABLE TestId2 (TestId2Id BIGINT PRIMARY KEY, Name VARCHAR(128));");
				command.Run("INSERT INTO TestId1 VALUES(123, 'Test')");
				
				// act
				installer.Install(0, 0, false, new Table(0, "TestId1"), new Table(1, "TestId2"));

				// assert
				command.CommandText = "SELECT * FROM " + TableName + " ORDER BY " + TableCommandBuilder.IdColumnName;
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
		public void Tables_without_primary_keys_should_cause_errors_if_flag_not_set()
		{
			// arrange
			var installer = new DefaultLongIdInstaller(DbHelper.ConnectionString,
				new TestDbMetadataProvider(new UniqueKey("dbo", "TestId1", "Id", false)), DbProviderName);
			
			// act + assert
			Assert.That(() => installer.Install(0, 0, false, new Table(0, "TestId1")),
				Throws.ArgumentException.And.Message.Contains("No key constraint found"));
		}

		[Test]
		public void Tables_with_only_unique_keys_should_not_cause_errors_if_flag_set()
		{
			// arrange
			var installer = new DefaultLongIdInstaller(DbHelper.ConnectionString,
				new TestDbMetadataProvider(new UniqueKey(string.Empty, "TestIdUniqueOnly", "Id", false)), DbProviderName);
			
			// act
			installer.Install(0, 0, true, new Table(0, "TestIdUniqueOnly"));

			// assert
			using (var connection = DbHelper.CreateConnection())
			{
				var command = connection.CreateCommand();
				var result = command.Run(string.Format("SELECT count(*) FROM {0} WHERE {1} = 'TestIdUniqueOnly'",
					TableName, TableCommandBuilder.TableNameColumnName), true);
				Assert.That(result, Is.EqualTo(1));
			}
		}

		[Test]
		public void Tables_with_no_keys_should_cause_errors()
		{
			// arrange
			var installer = new DefaultLongIdInstaller(DbHelper.ConnectionString, new TestDbMetadataProvider(), DbProviderName);

			// act + assert
			Assert.That(() => installer.Install(0, 0, false, new Table(0, "TestId1")),
				Throws.ArgumentException.And.Message.Contains("No key constraint found"));
		}

		[Test]
		public void Tables_with_no_matched_key_column_should_cause_errors()
		{
			// arrange
			var installer = new DefaultLongIdInstaller(DbHelper.ConnectionString,
				new TestDbMetadataProvider(new UniqueKey("dbo", "TestId1", "Id", true)), DbProviderName);

			// act + assert
			Assert.That(() => installer.Install(0, 0, false, new Table(0, "TestId1", keyColumnName: "TestId")),
				Throws.ArgumentException.And.Message.Contains("No key constraint found"));
		}
	}
}