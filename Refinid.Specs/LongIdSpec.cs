﻿using NUnit.Framework;
using RefinId;

namespace Refinid.Specs
{
	[TestFixture]
	public class LongIdSpec
	{
		[TestCase(0L)]
		[TestCase(-123L)]
		[TestCase(1L)]
		[TestCase(321L)]
		[TestCase(long.MaxValue)]
		[TestCase(long.MinValue)]
		[TestCase(long.MaxValue-1)]
		[TestCase(long.MinValue+1)]
		public void Implicit_conversions_should_set_value(long value)
		{
			LongId id = value;
			Assert.That(id.Value, Is.EqualTo(value));
			
			long longValue = id;
			Assert.That(longValue, Is.EqualTo(value));
		}

		[Test]
		public void Additional_fields_should_be_extracted_from_long()
		{
			LongId id = 0x1FEECCBB44332211;

			Assert.That(id.Type, Is.EqualTo(0x1FEE));
			Assert.That(id.Shard, Is.EqualTo(0xCC));
			Assert.That(id.Reserved, Is.EqualTo(0xBB));
		}
	}
}