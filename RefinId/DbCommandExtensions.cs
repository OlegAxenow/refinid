using System;
using System.Data;
using System.Data.Common;

namespace RefinId
{
	/// <summary>
	///     Extensions to prevent tunnel syndrome :)
	/// </summary>
	public static class DbCommandExtensions
	{
		/// <summary>
		///     Sets and executes <paramref name="commandText" /> immediately.
		///     Opens <see cref="DbCommand.Connection" /> if necessary.
		/// </summary>
		/// <exception cref="ArgumentNullException"> If a parameter not specified.</exception>
		public static object Run(this DbCommand command, string commandText, bool executeScalar = false)
		{
			if (command == null) throw new ArgumentNullException("command");
			if (commandText == null) throw new ArgumentNullException("commandText");

			if (command.Connection.State != ConnectionState.Open)
				command.Connection.Open();

			command.CommandType = CommandType.Text;
			command.CommandText = commandText;

			if (executeScalar)
				return command.ExecuteScalar();
			
			command.ExecuteNonQuery();
			return null;
		}
	}
}