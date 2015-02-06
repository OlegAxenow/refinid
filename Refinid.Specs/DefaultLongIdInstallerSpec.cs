using System;
using System.Data;
using System.Data.Common;
using NUnit.Framework;
using RefinId.Metadata;

namespace RefinId.Specs
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
		public void All_columns_should_be_created()
		{
			// arrange
			var installer = new DefaultLongIdInstaller(DbHelper.ConnectionString, new SQLiteDbMetadataProvider(), DbProviderName);
			using (var connection = DbHelper.CreateConnection())
			{
				connection.DropTableIfExists(TableName);
				var tableCommandBuilder = new TableCommandBuilder(DbHelper.ConnectionString, "System.Data.SQLite");
				var commandBuilder = tableCommandBuilder.InitializeCommandBuilderAndAdapter(connection);
				var dataSet = new DataSet();
				
				// act
				installer.Install(0, 0, false, null);

				// assert
				commandBuilder.DataAdapter.Fill(dataSet);

				foreach (var columnName in TableCommandBuilder.GetColumnNames())
				{
					Assert.That(dataSet.Tables[0].Columns.IndexOf(columnName) >= 0);
				}
			}
		}

		[Test]
		public void Type_should_be_unique_for_all_ids()
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
				command.Run("INSERT INTO TestId2 VALUES(1230, 'Test')");

				// act + assert
				Assert.That(() => installer.Install(0, 0, false, new Table(0, "TestId1"), new Table(1, "TestId2")), 
					Throws.ArgumentException.With.Message.StringContaining("already"));
			}
		}

		[Test]
		public void Last_id_should_be_loaded_from_table()
		{
			// arrange
			var installer = new DefaultLongIdInstaller(DbHelper.ConnectionString, new SQLiteDbMetadataProvider(), DbProviderName);
			using (var connection = DbHelper.CreateConnection())
			{
				var command = connection.CreateCommand();

				connection.DropTableIfExists(TableName);
				connection.DropTableIfExists("TestId1");
				command.Run("CREATE TABLE TestId1 (Id BIGINT PRIMARY KEY, Name VARCHAR(128));");
				command.Run("INSERT INTO TestId1 VALUES(123, 'Test')");
				
				// act
				installer.Install(0, 0, false, new Table(0, "TestId1"));

				// assert
				command.CommandText = "SELECT * FROM " + TableName;
				using (var reader = command.ExecuteReader())
				{
					int tableNameOrdinal = reader.GetOrdinal(TableCommandBuilder.TableNameColumnName);
					int idIndex = reader.GetOrdinal(TableCommandBuilder.IdColumnName);

					Assert.That(reader.Read());
					Assert.That(reader.GetString(tableNameOrdinal), Is.EqualTo("TestId1"));
					Assert.That(reader.GetInt64(idIndex), Is.EqualTo(123));

					Assert.That(!reader.Read());
				}
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
				
				// act
				installer.Install(11, 0, false, new Table(0, "TestId1"), new Table(1, "TestId2"));

				// assert
				command.CommandText = "SELECT * FROM " + TableName + " ORDER BY " + TableCommandBuilder.IdColumnName;
				using (var reader = command.ExecuteReader())
				{
					int tableNameOrdinal = reader.GetOrdinal(TableCommandBuilder.TableNameColumnName);
					int keyNameOrdinal = reader.GetOrdinal(TableCommandBuilder.KeyColumnName);
					int shardNameOrdinal = reader.GetOrdinal(TableCommandBuilder.ShardColumnName);

					Assert.That(reader.Read());
					Assert.That(reader.GetString(tableNameOrdinal), Is.EqualTo("TestId1"));
					Assert.That(reader.GetString(keyNameOrdinal), Is.EqualTo("Id"));
					Assert.That(reader.GetInt16(shardNameOrdinal), Is.EqualTo(11));

					Assert.That(reader.Read());
					Assert.That(reader.GetString(tableNameOrdinal), Is.EqualTo("TestId2"));
					Assert.That(reader.GetString(keyNameOrdinal), Is.EqualTo("TestId2Id"));
					Assert.That(reader.GetInt16(shardNameOrdinal), Is.EqualTo(11));

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