﻿using System.Data.Common;
using NUnit.Framework;

namespace RefinId.Specs
{
	public class BaseStorageSpec
	{
		protected const string DbProviderName = "System.Data.SQLite";
		protected const string TableName = TableCommandBuilder.DefaultTableName;

		[SetUp]
		public void SetUp()
		{
			using (DbConnection connection = DbHelper.CreateConnection())
			{
				var command = connection.CreateCommand();

				// create table to avoid problems with test metadata providers
				connection.DropTableIfExists(TableName);
				command.Run("CREATE TABLE " + TableName + " (" +
					TableCommandBuilder.TypeColumnName + " SMALLINT NOT NULL PRIMARY KEY, " +
					TableCommandBuilder.IdColumnName + " BIGINT NOT NULL, " +
					TableCommandBuilder.TableNameColumnName + " VARCHAR (128) NULL," +
					TableCommandBuilder.KeyColumnName + " VARCHAR (128) NULL," +
					TableCommandBuilder.ShardColumnName + " SMALLINT NULL)");
			}
		} 
	}
}