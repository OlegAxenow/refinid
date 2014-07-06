using System;
using System.Data;
using System.Data.Common;
using RefinId;

namespace Refinid.Specs
{
	/// <summary>
	/// Extensions to prevent tunnel syndrome :)
	/// </summary>
	public static class DbCommandExtensions
	{
		/// <summary>
		/// Sets and executes <paramref name="commandText"/> immediately.
		/// Opens <see cref="DbCommand.Connection"/> if necessary.
		/// </summary>
		/// <exception cref="ArgumentNullException"> If a parameter not specified.</exception>
		public static void Run(this DbCommand command, string commandText)
		{
			if (command == null) throw new ArgumentNullException("command");
			if (commandText == null) throw new ArgumentNullException("commandText");

			if (command.Connection.State != ConnectionState.Open)
				command.Connection.Open();

			command.CommandType = CommandType.Text;
			command.CommandText = commandText;
			command.ExecuteNonQuery();
		}

		/// <summary>
		/// Inserts <paramref name="initialId"/> into <paramref name="tableName"/>.
		/// </summary>
		/// <remarks> Expects that table have single column compatible with <see cref="long"/> type.
		/// </remarks>
		/// <exception cref="ArgumentNullException"> If a parameter not specified.</exception>		 
		public static void InsertInitialId(this DbCommand command, string tableName, long initialId)
		{
			if (tableName == null) throw new ArgumentNullException("tableName");
			command.Run("INSERT INTO " + tableName + 
				"VALUES (" + ((LongId)initialId).Type + "," + initialId + ")");
		}
	}
}