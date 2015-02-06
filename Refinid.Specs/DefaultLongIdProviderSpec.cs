using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using NUnit.Framework;

namespace RefinId.Specs
{
	[TestFixture]
	public class DefaultLongIdProviderSpec
	{
		[TestCase(0x0101AABB01010101, 0x0001AABB01010101, 0x0F01AABB01010101, 0x0201AABB01010101)]
		[TestCase(new[] { 0x0101AABB01010101L })]
		[TestCase]
		public void Last_values_should_be_correctly_loaded(params long[] values)
		{
			// arrange
			var storage = new TestStorage(values.ToArray());

			// act
			var provider = new DefaultLongIdProvider(storage);
			provider.FlushToStorage();

			// assert
			Assert.That(storage.Values, Is.EquivalentTo(values));
		}

		[Test]
		public void Create_should_increment_value()
		{
			// arrange
			const long InitialValue = 0x0101AABB01010101;
			var storage = new TestStorage(InitialValue);
			var provider = new DefaultLongIdProvider(storage);

			// act + assert
			Assert.That(() => provider.Create(0x0101), Is.EqualTo(InitialValue + 1));
			Assert.That(() => provider.Create(0x0101), Is.EqualTo(InitialValue + 2));
		}

		[Test]
		public void Create_should_not_produce_exact_number_of_different_values_in_different_threads()
		{
			// arrange
			const int Times = 100;
			const long InitialValue = 0x0101AABB01010101;

			var storage = new TestStorage(InitialValue);
			var provider = new DefaultLongIdProvider(storage);

			var queue = new ConcurrentQueue<long>();

			// act
			new MultiThreadTestRunner(() =>
			{
				long id = provider.Create(0x0101);
				queue.Enqueue(id);

				Debug.WriteLine(id);
				Assert.That(id, Is.GreaterThan(InitialValue));
			}).Run(Times, 0);

			// assert
			var allIds = new HashSet<long>();
			long result;
			long maxResult = 0;
			while (queue.TryDequeue(out result))
			{
				// exception if already taken
				allIds.Add(result);
				if (maxResult < result)
					maxResult = result;
			}

			Assert.That(allIds.Count, Is.EqualTo(Times));
			Assert.That(maxResult - InitialValue, Is.EqualTo(Times));
		}
	}
}