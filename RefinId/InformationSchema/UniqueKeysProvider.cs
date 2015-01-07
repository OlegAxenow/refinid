using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace RefinId.InformationSchema
{
	/// <summary>
	///     Provides information about primary keys.
	/// </summary>
	public class UniqueKeysProvider : IUniqueKeysProvider
	{
		private const string UniqueKeysCommandText = @"select t.*, c.DATA_TYPE as DataType from
(select kcu.TABLE_SCHEMA as SchemaName, kcu.TABLE_NAME as TableName,
		max(kcu.COLUMN_NAME) as ColumnName, tc.CONSTRAINT_TYPE as ConstraintType, COUNT(*) as ColumnCount
	from INFORMATION_SCHEMA.TABLE_CONSTRAINTS as tc
	join INFORMATION_SCHEMA.KEY_COLUMN_USAGE as kcu
		on kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA and kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
		and kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA and kcu.TABLE_NAME = tc.TABLE_NAME
 where tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
 group by tc.CONSTRAINT_TYPE, kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.COLUMN_NAME) t
 join INFORMATION_SCHEMA.COLUMNS as c 
	on c.TABLE_SCHEMA = t.SchemaName and c.TABLE_NAME = t.TableName and c.COLUMN_NAME = t.ColumnName";

		private const string PrimaryKeyConstraintType = "PRIMARY KEY";

		private const int SchemaOrdinal = 0;

		private const int TableNameOrdinal = 1;

		private const int ColumnNameOrdinal = 2;

		private const int ConstraintTypeOrdinal = 3;

		private const int ColumnCountOrdinal = 4;

		private const int DataTypeOrdinal = 5;

		/// <summary>
		///     Returns <see cref="UniqueKey" /> instances for all unique and primary key constraints for current database,.
		/// </summary>
		/// <param name="command"> <see cref="DbCommand" /> with open connection to use for constraints retrieving.</param>
		public IEnumerable<UniqueKey> GetUniqueKeys(DbCommand command)
		{
			if (command == null) throw new ArgumentNullException("command");
			if (command.Connection == null || command.Connection.State != ConnectionState.Open)
				throw new ArgumentException();

			command.CommandText = UniqueKeysCommandText;
			command.CommandType = CommandType.Text;

			using (DbDataReader reader = command.ExecuteReader())
			{
				while (reader.Read())
				{
					yield return new UniqueKey(
						reader.GetString(SchemaOrdinal), reader.GetString(TableNameOrdinal), reader.GetString(ColumnNameOrdinal),
						reader.GetString(ConstraintTypeOrdinal).Equals(PrimaryKeyConstraintType, StringComparison.OrdinalIgnoreCase),
						reader.GetInt32(ColumnCountOrdinal), reader.GetString(DataTypeOrdinal));
				}
			}
		}
	}
}