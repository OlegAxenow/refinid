using System.Runtime.InteropServices;

namespace RefinId
{
	/// <summary>
	/// Struct to hold long identifier.
	/// </summary>
	[StructLayout(LayoutKind.Explicit)]
	public struct LongId
	{
		public static implicit operator long(LongId id)
		{
			return id.Value;
		}

		public static implicit operator LongId(long id)
		{
			return new LongId { Value = id };
		}

		/// <summary>
		/// Long identifier itself (as stored in database).
		/// </summary>
		[FieldOffset(0)]
		public long Value;

		/// <summary>
		/// Type of the entity, presented by this identifier.
		/// NB:	For user types should not be zero (reserved for internal purposes).
		/// Extracted from first two bytes of <see cref="Value"/>.
		/// </summary>
		[FieldOffset(6)]
		public ushort Type;

		/// <summary>
		/// Optional shard number to distinguish identifiers, created from different shards.
		/// </summary>
		[FieldOffset(5)]
		public byte Shard;

		/// <summary>
		/// Reserved for any purpose (5-byte for identifier, 2-byte shard etc.).
		/// </summary>
		[FieldOffset(4)]
		public byte Reserved;
	}
}