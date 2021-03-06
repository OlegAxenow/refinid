﻿using System;
using System.Collections.Generic;
using System.Data.Common;
using NUnit.Framework;

namespace RefinId.Specs
{
	[TestFixture]
	public class DbLongIdStorageSpec : BaseStorageSpec
	{
		/// <summary>
		///     Inserts <paramref name="initialId" /> into <paramref name="lastIdentifiersTableName" />.
		/// </summary>
		/// <exception cref="ArgumentNullException"> If a parameter not specified.</exception>
		public static void InsertInitialId(DbCommand command, string lastIdentifiersTableName,
			long initialId, string tableNameForType, string keyColumnName, byte shard = 0)
		{
			if (command == null) throw new ArgumentNullException("command");
			if (lastIdentifiersTableName == null) throw new ArgumentNullException("lastIdentifiersTableName");
			if (tableNameForType == null) throw new ArgumentNullException("tableNameForType");
			command.Run("INSERT INTO " + lastIdentifiersTableName +
			            " VALUES (" + ((LongId)initialId).Type + "," + initialId + ",'" +
			            tableNameForType + "', '" + keyColumnName + "', " + shard + ")");
		}

		[Test]
		public void Saved_should_reject_non_unique_types()
		{
			// arrange
			const long NewId1 = 0x1FEECCBB44332211;
			const long NewId2 = 0x3FEECCBB44332211;
			var valuesWithNonUniqueTypes = new[] { NewId1 + 1, NewId2 + 2, NewId1 + 3 };

			var storage = new DbLongIdStorage(DbHelper.ConnectionString, DbProviderName);

			// act + assert
			Assert.That(() => storage.SaveLastValues(valuesWithNonUniqueTypes),
				Throws.ArgumentException);
		}

		[Test]
		public void Saved_should_reject_null_values()
		{
			// arrange
			var storage = new DbLongIdStorage(DbHelper.ConnectionString, DbProviderName);

			// act + assert
			Assert.That(() => storage.SaveLastValues(null), Throws.InstanceOf<ArgumentNullException>());
		}

		[Test]
		public void Values_should_be_loaded_from_real_table()
		{
			// arrange
			using (DbConnection connection = DbHelper.CreateConnection())
			{
				DbCommand command = connection.CreateCommand();
				command.Run("CREATE TABLE TestRealId (Id BIGINT PRIMARY KEY, Name VARCHAR(128));");
				command.Run("INSERT INTO TestRealId VALUES(15, 'test');");
				InsertInitialId(command, TableName, 1, "TestRealId", "Id");
				
				var storage = new DbLongIdStorage(DbHelper.ConnectionString, DbProviderName);

				// act
				List<long> lastValues = storage.GetLastValues(true);

				// assert
				Assert.That(lastValues, Is.EquivalentTo(new[] { 15 }));

				// cleanup
				command.Run("DELETE FROM " + TableName);
				command.Run("DROP TABLE TestRealId ");
			}
		}

		[Test]
		public void Values_should_be_saved_to_database()
		{
			// arrange
			const long InitialId1 = 0x1FEECCBB44332211; // update
			const long InitialId2 = 0x2FEECCBB44332211; // delete
			const long NewId3 = 0x3FEECCBB44332211; // insert

			using (DbConnection connection = DbHelper.CreateConnection())
			{
				DbCommand command = connection.CreateCommand();
				InsertInitialId(command, TableName, InitialId1, "fake_table", "fake_id");
				InsertInitialId(command, TableName, InitialId2, "fake_table", "fake_id");

				var storage = new DbLongIdStorage(DbHelper.ConnectionString, DbProviderName);

				// act
				storage.SaveLastValues(new[] { InitialId1 + 1, NewId3 + 3 });
				// TODO: to better isolation use direct reading from database instead of storage
				List<long> lastValues = storage.GetLastValues();

				// assert
				Assert.That(lastValues, Is.EquivalentTo(new[] { InitialId1 + 1, NewId3 + 3 }));

				// cleanup
				command.Run("DELETE FROM " + TableName);
			}
		}

		[Test]
		public void Values_should_be_loaded_from_database()
		{
			// arrange
			const long InitialId1 = 0x1FEECCBB44332211;
			const long InitialId2 = 0x2FEECCBB44332211;

			using (DbConnection connection = DbHelper.CreateConnection())
			{
				DbCommand command = connection.CreateCommand();
				InsertInitialId(command, TableName, InitialId1, "fake_table", "fake_id");
				InsertInitialId(command, TableName, InitialId2, "fake_table", "fake_id");

				var storage = new DbLongIdStorage(DbHelper.ConnectionString, DbProviderName);

				// act
				List<long> lastValues = storage.GetLastValues();

				// assert
				Assert.That(lastValues, Is.EquivalentTo(new[] { InitialId1, InitialId2 }));

				// cleanup
				command.Run("DELETE FROM " + TableName);
			}
		}
	}
}