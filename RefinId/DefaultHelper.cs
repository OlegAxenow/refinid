﻿using System;
using RefinId.Metadata;

namespace RefinId
{
	/// <summary>
	/// Simplifies operations for default settings.
	/// </summary>
	public class DefaultHelper
	{
		private readonly string _connectionString;
		private readonly string _dbProviderName;
		private readonly string _configurationTableName;
		private readonly IDbMetadataProvider _dbMetadataProvider;
		private readonly Lazy<DefaultLongIdProvider> _provider;

		/// <summary>
		/// Store parameters for future use.
		/// </summary>
		public DefaultHelper(string connectionString, string dbProviderName, string configurationTableName = null, IDbMetadataProvider dbMetadataProvider = null)
		{
			_connectionString = connectionString;
			_dbProviderName = dbProviderName;
			_configurationTableName = configurationTableName;
			_dbMetadataProvider = dbMetadataProvider;

			_provider = new Lazy<DefaultLongIdProvider>(() => new DefaultLongIdProvider(GetStorage()));
		}

		private ILongIdStorage GetStorage()
		{
			return new DbLongIdStorage(_connectionString, _dbProviderName, _configurationTableName);
		}

		/// <summary>
		/// Returns <see cref="DefaultLongIdProvider"/> singleton (creates if needed).
		/// </summary>
		public ILongIdProvider GetProvider()
		{
			return _provider.Value;
		}

		/// <summary>
		/// Creates <see cref="DefaultLongIdInstaller"/> and pass default parameters to <see cref="DefaultLongIdInstaller.Install"/>.
		/// </summary>
		public void Install(byte shard, byte reserved, bool useUniqueIfPrimaryKeyNotMatch, params Table[] tables)
		{
			var storage = new DbLongIdStorage(_connectionString, _dbProviderName, _configurationTableName);
			var installer = new DefaultLongIdInstaller(_dbMetadataProvider ?? new DbMetadataProvider(), storage);
			installer.Install(shard, reserved, useUniqueIfPrimaryKeyNotMatch, tables);
		}
	}
}