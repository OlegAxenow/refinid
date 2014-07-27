﻿using System;

namespace RefinId
{
	/// <summary>
	///     Contains table's properties for installers and storages.
	/// </summary>
	public class Table : ISchemaAndTable
	{
		/// <summary>
		///     Default value for <see cref="Schema" />, if not specified in constructor.
		/// </summary>
		private const string DefaultSchema = "dbo";

		/// <summary>
		///     Creates instance with specified parameters.
		/// </summary>
		/// <param name="typeId"> Type's identifier for current table (corresponds to <see cref="LongId.Type" />.</param>
		/// <param name="tableName"> Unquoted table's name.</param>
		/// <param name="schema"> Unquoted table's schema (<see cref="DefaultSchema" />, by default).</param>
		/// <param name="keyColumnName"> Optional unquoted key column's name.</param>
		public Table(short typeId, string tableName, string schema = null, string keyColumnName = null)
		{
			if (tableName == null) throw new ArgumentNullException("tableName");
			KeyColumnName = keyColumnName;
			TypeId = typeId;
			TableName = tableName;
			Schema = schema ?? DefaultSchema;
		}

		/// <summary>
		///     Type's identifier for current table (corresponds to <see cref="LongId.Type" />.
		/// </summary>
		public short TypeId { get; private set; }

		/// <summary>
		///     Unquoted table's schema ("dbo", by default).
		/// </summary>
		public string Schema { get; private set; }

		/// <summary>
		///     Unquoted table's name.
		/// </summary>
		public string TableName { get; private set; }

		/// <summary>
		///     Optional unquoted key column's name.
		/// </summary>
		/// <remarks> If not specified, most appropriate key column used by installer.</remarks>
		public string KeyColumnName { get; private set; }
	}
}