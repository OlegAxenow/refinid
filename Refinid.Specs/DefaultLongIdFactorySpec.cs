using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using NUnit.Framework;
using RefinId;

namespace Refinid.Specs
{
	[TestFixture]
	public class DefaultLongIdFactorySpec
	{
		[TestCase(0x0101AABB01010101, 0x0001AABB01010101, 0x0F01AABB01010101, 0x0201AABB01010101)]
		[TestCase(new[] { 0x0101AABB01010101L })]
		[TestCase]
		public void Last_values_should_be_correctly_loaded(params long[] values)
		{
			// arrange
			var storage = new TestStorage(values.ToArray());

			// act
			var factory = new DefaultLongIdFactory(storage);
			factory.FlushToStorage();

			// assert
			Assert.That(storage.Values, Is.EquivalentTo(values));
		}

		[Test]
		public void Create_should_increment_value()
		{
			// arrange
			const long initialValue = 0x0101AABB01010101;
			var storage = new TestStorage(initialValue);
			var factory = new DefaultLongIdFactory(storage);

			// act + assert
			Assert.That(() => factory.Create(0x0101), Is.EqualTo(initialValue + 1));
			Assert.That(() => factory.Create(0x0101), Is.EqualTo(initialValue + 2));
		}

		[Test]
		public void Create_should_not_produce_exact_number_of_different_values_in_different_threads()
		{
			// arrange
			const int times = 100;
			const long initialValue = 0x0101AABB01010101;

			var storage = new TestStorage(initialValue);
			var factory = new DefaultLongIdFactory(storage);

			var queue = new ConcurrentQueue<long>();

			// act
			new MultiThreadTestRunner(() =>
			{
				long id = factory.Create(0x0101);
				queue.Enqueue(id);

				Debug.WriteLine(id);
				Assert.That(id, Is.GreaterThan(initialValue));
			}).Run(times, 0);

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

			Assert.That(allIds.Count, Is.EqualTo(times));
			Assert.That(maxResult - initialValue, Is.EqualTo(times));
		}
	}
}