**This readme file is copied and reformatted from SpookyHash's [home](https://burtleburtle.net/bob/hash/spooky.html) page.**

SpookyHash: a 128-bit noncryptographic hash
===========================================

SpookyHash is a public domain noncryptographic hash function producing well-distributed 128-bit hash values for byte arrays of any length. It can produce 64-bit and 32-bit hash values too, at the same speed, just use the bottom n bits.

This C++ implementation (`SpookyV2.h` and `SpookyV2.cpp`) is specific to 64-bit x86 platforms, in particular it assumes the processor is little endian. Long keys hash in 3 bytes per cycle, short keys take about 1 byte per cycle, and there is a 30 cycle startup cost. Keys can be supplied in fragments. The function allows a 128-bit seed. It's named SpookyHash because it was released on Halloween.

Why use SpookyHash?
-------------------

* It's fast. For short keys it's 1 byte per cycle, with a 30 cycle startup cost. For long keys, well, it would be 3 bytes per cycle, and that only occupies one core. Except you'll hit the limit of how fast uncached memory can be read, which on my home machines is less than 2 bytes per cycle.
* It's good. It achieves avalanche for 1-bit and 2-bit inputs. It works for any type of key that can be made to look like an array of bytes, or list of arrays of bytes. It takes a seed, so it can produce many different independent hashes for the same key.
* It can produce up to 128-bit results. Large systems should consider using 128-bit checksums nowadays. A library with 4 billion documents is expected to have about 1 colliding 64-bit checksum no matter how good the hash is. Libraries using 128-bit checksums should expect 1 collision once they hit 16 quintillion documents. (Due to limited hardware, I can only verify SpookyHash is as good as a good 73-bit checksum. It might be better, I can't tell.)
* When NOT to use it: if you have an opponent. This is not cryptographic. Given a message, a resourceful opponent could write a tool that would produce a modified message with the same hash as the original message. Once such a tool is written, a not-so-resourceful opponent could borrow the tool and do the same thing.
* Another case not to use it: CRCs have a nice property that you can split a message up into pieces arbitrarily, calculate the CRC all the pieces, then aftewards combine the CRCs for the pieces to find the CRC for the concatenation of all those pieces. SpookyHash can't. If you could deterministically choose what the pieces were, though, you could compute the hashes for pieces with SpookyHash (or CityHash or any other hash), then treat those hash values as the raw data, and do CRCs on top of that.
* Big-endian machines aren't supported by the current implementation. The hash would run, and it would produce equally good results, but they'd be different results from little-endian platforms. Machines that can't handle unaligned reads also won't work by default, but there's a macro in the implementation to tweak that will let it deal with unaligned reads. x86-compatible machines are little-endian and support unaligned reads.

Notes
-----

* For long keys, the inner loop of SpookyHash is Spooky::Mix(). It consumes an 8-byte input, then does an xor, another xor, a rotation, and an addition. The internal state won't entirely fit in registers after 3 or 4 8-byte variables. But parallelism keeps increasing, and so does avalanche per pass. There was a sweet spot around 12 variables.
* I tried SSE2 instructions, and 64-bit multiplications, but it turned out that plain 64-bit rotate, addition, and XOR got results faster than those. I thought 4 or 6 variable schemes were going to win, but 12 variables won in the end. My boss suggests I look into GPUs. I haven't tried that yet. But given that the memory bandwidth is maxed out, I doubt they would help.
* While Spooky::Mix() handles the middle of long keys well, it would need 4 repetitions for the final mix, and it has a huge startup cost. That would make short keys expensive. So I found the ShortHash to produce a 128-bit hash of short keys with little startup cost, and Spooky::End() to reduce the final mixing cost (this shows up most for keys just over 192 bytes long). Those components aren't trickle-feed, they work the old way. ShortHash is used automatically for short keys.
* I have not tried the CRC32 instruction that started in the Nehalem Intel chips, because I don't have one. Google has, with CityHash. They claim 6 bytes per cycle, which is faster than any hash I've seen. On my machine CityHash about half the speed of SpookyHash; mine doesn't have a CRC32 instruction. CityHash passes my frog test for at least 272 keypairs.
