using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using NUnit.Framework;
using RefinId;

namespace Refinid.Specs
{
	[TestFixture]
	public class DbLongIdStorageSpec
	{
		private const string ConnectionString =
			"Server=(localdb)\\v11.0;Integrated Security=true;Database=" + TestDatabaseName;

		private const string TestDatabaseName = "RefineIdTest";
		private const string TableName = "[" + DbLongIdStorage.DefaultTableName + "]";

		[TestFixtureSetUp]
		public void TestFixtureSetUp()
		{
			using (SqlConnection connection = CreateConnection())
			{
				SqlCommand command = connection.CreateCommand();
				try
				{
					command.Run("CREATE DATABASE " + TestDatabaseName);
				} // ReSharper disable once EmptyGeneralCatchClause
				catch
				{
				}

				UseTestDatabase(command);

				command.Run("IF OBJECT_ID('" + TableName + "') IS NULL " +
				            " CREATE TABLE " + TableName + " (" + DbLongIdStorage.TypeColumnName +
							" smallint not null primary key, " + DbLongIdStorage.IdColumnName +
							" bigint not null);");

				command.Run("DELETE FROM " + TableName);
			}
		}

		private static void UseTestDatabase(SqlCommand command)
		{
			command.Run("USE " + TestDatabaseName);
		}

		private static SqlConnection CreateConnection()
		{
			return new SqlConnection(ConnectionString);
		}

		[Test]
		public void Values_should_be_loaded_from_database()
		{
			// arrange
			const long initialId1 = 0x1FEECCBB44332211;
			const long initialId2 = 0x2FEECCBB44332211;

			using (SqlConnection connection = CreateConnection())
			{
				var command = connection.CreateCommand();
				command.InsertInitialId(TableName, initialId1);
				command.InsertInitialId(TableName, initialId2);

				var storage = new DbLongIdStorage(ConnectionString);

				// act
				List<long> lastValues = storage.GetLastValues();

				// assert
				Assert.That(lastValues, Is.EquivalentTo(new[] { initialId1, initialId2 }));

				// cleanup
				command.Run("DELETE FROM " + TableName);
			}
		}

		[Test]
		public void Values_should_be_saved_to_database()
		{
			// arrange
			const long initialId1 = 0x1FEECCBB44332211;	// update
			const long initialId2 = 0x2FEECCBB44332211;	// delete
			const long newId3 = 0x3FEECCBB44332211;		// insert

			using (SqlConnection connection = CreateConnection())
			{
				var command = connection.CreateCommand();
				command.InsertInitialId(TableName, initialId1);
				command.InsertInitialId(TableName, initialId2);

				var storage = new DbLongIdStorage(ConnectionString);

				// act
				storage.SaveLastValues(new[] { initialId1 + 1, newId3 + 3 });
				// TODO: to better isolation use direct reading from database instead of storage
				List<long> lastValues = storage.GetLastValues();

				// assert
				Assert.That(lastValues, Is.EquivalentTo(new[] { initialId1 + 1, newId3 + 3 }));

				// cleanup
				command.Run("DELETE FROM " + TableName);
			}
		}

		[Test]
		public void Saved_should_reject_null_values()
		{
			// arrange
			var storage = new DbLongIdStorage(ConnectionString);

			// act + assert
			Assert.That(() => storage.SaveLastValues(null), Throws.InstanceOf<ArgumentNullException>());
		}

		[Test]
		public void Saved_should_reject_non_unique_types()
		{
			// arrange
			const long newId1 = 0x1FEECCBB44332211;
			const long newId2 = 0x3FEECCBB44332211;
			var valuesWithNonUniqueTypes = new[] { newId1 + 1, newId2 + 2, newId1 + 3 };

			using (SqlConnection connection = CreateConnection())
			{
				var storage = new DbLongIdStorage(ConnectionString);

				// act + assert
				Assert.That(() => storage.SaveLastValues(valuesWithNonUniqueTypes),
					Throws.ArgumentException);
			}
		}
	}
}