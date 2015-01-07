## RefinId
RefinId - fast and compact 8-byte substitution of GUID for your identifiers with built-in entity types support. You can use it for your Microsoft.NET data access code or something else.

## Goals
For my .NET projects I wanted to use something faster than GUID with ability to determine entity type from identifier itself. I considered sharding and lazy writing support as a nice addition.

## Why not GUID?

Sometimes you may consider using GUID for your identifiers (regardless concrete database). But wait, do you know about performance degradation as a result of this choice?
Let me show you problems with GUID as a primary key, especially when key is clustered index, for Microsoft SQL Server comparing to "long" 8-byte identifier.

1. GUID is takes 16 byte. Use 16 bytes or 8 bytes – not such a big difference, huh? Remember, that primary key will be used in all child tables and clustered key – in all non-clustered indexes. Not a big deal when tables small enough, but for big tables… it depends.
2. If you generate GUID in code or with NEWID() SQL function you get random keys and, as a result, frequent page splitting and index fragmentation.

You can find more thorough explanation [here](http://www.sqlskills.com/blogs/kimberly/guids-as-primary-keys-andor-the-clustering-key/).

## Getting started

### How to get

TBD (building, NuGet)

### How to use

For simple situation you can use something like this:

	var installer = new SqlClientLongIdInstaller(connectionString,	new UniqueKeysProvider());
	installer.Install(0, 0, false, new Table(1, "Test1"), new Table(2, "TestId2"));
	...
	var storage = new DbLongIdStorage(connectionString);
	var factory = new DefaultLongIdFactory(storage);
	...
	short type = 1;
	long id = factory.Create(type);

## How it works

### LongId

I decide that for my goals 8-byte structure with LayoutKind.Explicit will be good enough to hold Value, Type and Shard fields. Also I reserved single byte for future use (e.g. for extending one of other fields).
Then, I wrote implicit operators to convert to and from long (and trivial Equals and GetHashCode implementation).
You can take a look on code from unit-test to see how to use it:
	LongId id = 0x1FEECCBB44332211;

	Assert.That(id.Value, Is.EqualTo(0x44332211));
	Assert.That(id.Type, Is.EqualTo(0x1FEE));
	Assert.That(id.Shard, Is.EqualTo(0xCC));
	Assert.That(id.Reserved, Is.EqualTo(0xBB));

### DefaultLongIdFactory

*DefaultLongIdFactory* provides Create method to retrieve new identifier for specified entity type. This factory (surprise!) is thread-safe.

Two things are not trivial, let me explain it:
1. It uses simple Dictionary, because all writing to dictionary performed inside the constructor.
2. It uses private IdWrapper class. If we use long without wrapper, we cannot use *ref* for *Interlocked.Increment(ref long)*.


## Requirements and dependencies

License: [MIT](http://opensource.org/licenses/MIT).

The source code depends on following NuGet packages:

- NUnit (only for Refinid.Specs)
- Moq (only for Refinid.Specs)