/*
 * xxHash - Extremely Fast Hash algorithm
 * Copyright (C) 2012-2020 Yann Collet
 *
 * BSD 2-Clause License (https://www.opensource.org/licenses/bsd-license.php)
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    * Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 *      copyright notice, this list of conditions and the following disclaimer
 *      in the documentation and/or other materials provided with the
 *      distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * You can contact the author at:
 *   - xxHash homepage: https://www.xxhash.com
 *   - xxHash source repository: https://github.com/Cyan4973/xxHash
 */

/*
 * xxhash.c instantiates functions defined in xxhash.h
 */

#define XXH_STATIC_LINKING_ONLY /* access advanced declarations */
#define XXH_IMPLEMENTATION /* access definitions */

#include "flow/xxhash.h"
#include "cpuinfo_x86.h"

#if defined(__AVX512F__)
#include <immintrin.h>
#endif

// Runtime function pointers
XXH3_f_accumulate_512 XXH3_accumulate_512 = NULL;
XXH3_f_scrambleAcc XXH3_scrambleAcc = NULL;
XXH3_f_initCustomSecret XXH3_initCustomSecret = NULL;

int cpu_has_avx512(void) {
	static int cached = -1;
	if (cached == -1) {
		X86Features features = GetX86Info().features;
		cached = features.avx512f && features.avx512bw && features.avx512dq;
	}
	return cached;
}

int cpu_has_avx2(void) {
	static int cached = -1;
	if (cached == -1) {
		cached = GetX86Info().features.avx2;
	}
	return cached;
}

int cpu_has_sse2(void) {
	static int cached = -1;
	if (cached == -1) {
		cached = GetX86Info().features.sse2;
	}
	return cached;
}

void XXH3_accumulate_512_avx512(void* XXH_RESTRICT acc,
                                const void* XXH_RESTRICT input,
                                const void* XXH_RESTRICT secret) {
	XXH_ALIGN(64) __m512i* const xacc = (__m512i*)acc;
	XXH_ASSERT((((size_t)acc) & 63) == 0);
	XXH_STATIC_ASSERT(XXH_STRIPE_LEN == sizeof(__m512i));

	{
		/* data_vec    = input[0]; */
		__m512i const data_vec = _mm512_loadu_si512(input);
		/* key_vec     = secret[0]; */
		__m512i const key_vec = _mm512_loadu_si512(secret);
		/* data_key    = data_vec ^ key_vec; */
		__m512i const data_key = _mm512_xor_si512(data_vec, key_vec);
		/* data_key_lo = data_key >> 32; */
		__m512i const data_key_lo = _mm512_shuffle_epi32(data_key, (_MM_PERM_ENUM)_MM_SHUFFLE(0, 3, 0, 1));
		/* product     = (data_key & 0xffffffff) * (data_key_lo & 0xffffffff); */
		__m512i const product = _mm512_mul_epu32(data_key, data_key_lo);
		/* xacc[0] += swap(data_vec); */
		__m512i const data_swap = _mm512_shuffle_epi32(data_vec, (_MM_PERM_ENUM)_MM_SHUFFLE(1, 0, 3, 2));
		__m512i const sum = _mm512_add_epi64(*xacc, data_swap);
		/* xacc[0] += product; */
		*xacc = _mm512_add_epi64(product, sum);
	}
}

void XXH3_scrambleAcc_avx512(void* XXH_RESTRICT acc, const void* XXH_RESTRICT secret) {
	XXH_ASSERT((((size_t)acc) & 63) == 0);
	XXH_STATIC_ASSERT(XXH_STRIPE_LEN == sizeof(__m512i));
	{
		XXH_ALIGN(64) __m512i* const xacc = (__m512i*)acc;
		const __m512i prime32 = _mm512_set1_epi32((int)XXH_PRIME32_1);

		/* xacc[0] ^= (xacc[0] >> 47) */
		__m512i const acc_vec = *xacc;
		__m512i const shifted = _mm512_srli_epi64(acc_vec, 47);
		__m512i const data_vec = _mm512_xor_si512(acc_vec, shifted);
		/* xacc[0] ^= secret; */
		__m512i const key_vec = _mm512_loadu_si512(secret);
		__m512i const data_key = _mm512_xor_si512(data_vec, key_vec);

		/* xacc[0] *= XXH_PRIME32_1; */
		__m512i const data_key_hi = _mm512_shuffle_epi32(data_key, (_MM_PERM_ENUM)_MM_SHUFFLE(0, 3, 0, 1));
		__m512i const prod_lo = _mm512_mul_epu32(data_key, prime32);
		__m512i const prod_hi = _mm512_mul_epu32(data_key_hi, prime32);
		*xacc = _mm512_add_epi64(prod_lo, _mm512_slli_epi64(prod_hi, 32));
	}
}

void XXH3_initCustomSecret_avx512(void* XXH_RESTRICT customSecret, xxh_u64 seed64) {
	XXH_STATIC_ASSERT((XXH_SECRET_DEFAULT_SIZE & 63) == 0);
	XXH_STATIC_ASSERT(XXH_SEC_ALIGN == 64);
	XXH_ASSERT(((size_t)customSecret & 63) == 0);
	(void)(&XXH_writeLE64);
	{
		int const nbRounds = XXH_SECRET_DEFAULT_SIZE / sizeof(__m512i);
		__m512i const seed = _mm512_mask_set1_epi64(_mm512_set1_epi64((xxh_i64)seed64), 0xAA, -(xxh_i64)seed64);

		XXH_ALIGN(64) const __m512i* const src = (const __m512i*)XXH3_kSecret;
		XXH_ALIGN(64) __m512i* const dest = (__m512i*)customSecret;
		int i;
		for (i = 0; i < nbRounds; ++i) {
			/* GCC has a bug, _mm512_stream_load_si512 accepts 'void*', not 'void const*',
			 * this will warn "discards ‘const’ qualifier". */
			union {
				XXH_ALIGN(64) const __m512i* cp;
				XXH_ALIGN(64) void* p;
			} remote_const_void;
			remote_const_void.cp = src + i;
			dest[i] = _mm512_add_epi64(_mm512_stream_load_si512(remote_const_void.p), seed);
		}
	}
}

void XXH3_accumulate_512_avx2(void* XXH_RESTRICT acc, const void* XXH_RESTRICT input, const void* XXH_RESTRICT secret) {
	XXH_ASSERT((((size_t)acc) & 31) == 0);
	{
		XXH_ALIGN(32) __m256i* const xacc = (__m256i*)acc;
		/* Unaligned. This is mainly for pointer arithmetic, and because
		 * _mm256_loadu_si256 requires  a const __m256i * pointer for some reason. */
		const __m256i* const xinput = (const __m256i*)input;
		/* Unaligned. This is mainly for pointer arithmetic, and because
		 * _mm256_loadu_si256 requires a const __m256i * pointer for some reason. */
		const __m256i* const xsecret = (const __m256i*)secret;

		size_t i;
		for (i = 0; i < XXH_STRIPE_LEN / sizeof(__m256i); i++) {
			/* data_vec    = xinput[i]; */
			__m256i const data_vec = _mm256_loadu_si256(xinput + i);
			/* key_vec     = xsecret[i]; */
			__m256i const key_vec = _mm256_loadu_si256(xsecret + i);
			/* data_key    = data_vec ^ key_vec; */
			__m256i const data_key = _mm256_xor_si256(data_vec, key_vec);
			/* data_key_lo = data_key >> 32; */
			__m256i const data_key_lo = _mm256_shuffle_epi32(data_key, _MM_SHUFFLE(0, 3, 0, 1));
			/* product     = (data_key & 0xffffffff) * (data_key_lo & 0xffffffff); */
			__m256i const product = _mm256_mul_epu32(data_key, data_key_lo);
			/* xacc[i] += swap(data_vec); */
			__m256i const data_swap = _mm256_shuffle_epi32(data_vec, _MM_SHUFFLE(1, 0, 3, 2));
			__m256i const sum = _mm256_add_epi64(xacc[i], data_swap);
			/* xacc[i] += product; */
			xacc[i] = _mm256_add_epi64(product, sum);
		}
	}
}

void XXH3_scrambleAcc_avx2(void* XXH_RESTRICT acc, const void* XXH_RESTRICT secret) {
	XXH_ASSERT((((size_t)acc) & 31) == 0);
	{
		XXH_ALIGN(32) __m256i* const xacc = (__m256i*)acc;
		/* Unaligned. This is mainly for pointer arithmetic, and because
		 * _mm256_loadu_si256 requires a const __m256i * pointer for some reason. */
		const __m256i* const xsecret = (const __m256i*)secret;
		const __m256i prime32 = _mm256_set1_epi32((int)XXH_PRIME32_1);

		size_t i;
		for (i = 0; i < XXH_STRIPE_LEN / sizeof(__m256i); i++) {
			/* xacc[i] ^= (xacc[i] >> 47) */
			__m256i const acc_vec = xacc[i];
			__m256i const shifted = _mm256_srli_epi64(acc_vec, 47);
			__m256i const data_vec = _mm256_xor_si256(acc_vec, shifted);
			/* xacc[i] ^= xsecret; */
			__m256i const key_vec = _mm256_loadu_si256(xsecret + i);
			__m256i const data_key = _mm256_xor_si256(data_vec, key_vec);

			/* xacc[i] *= XXH_PRIME32_1; */
			__m256i const data_key_hi = _mm256_shuffle_epi32(data_key, _MM_SHUFFLE(0, 3, 0, 1));
			__m256i const prod_lo = _mm256_mul_epu32(data_key, prime32);
			__m256i const prod_hi = _mm256_mul_epu32(data_key_hi, prime32);
			xacc[i] = _mm256_add_epi64(prod_lo, _mm256_slli_epi64(prod_hi, 32));
		}
	}
}

void XXH3_initCustomSecret_avx2(void* XXH_RESTRICT customSecret, xxh_u64 seed64) {
	XXH_STATIC_ASSERT((XXH_SECRET_DEFAULT_SIZE & 31) == 0);
	XXH_STATIC_ASSERT((XXH_SECRET_DEFAULT_SIZE / sizeof(__m256i)) == 6);
	XXH_STATIC_ASSERT(XXH_SEC_ALIGN <= 64);
	(void)(&XXH_writeLE64);
	XXH_PREFETCH(customSecret);
	{
		__m256i const seed = _mm256_set_epi64x(-(xxh_i64)seed64, (xxh_i64)seed64, -(xxh_i64)seed64, (xxh_i64)seed64);

		XXH_ALIGN(64) const __m256i* const src = (const __m256i*)XXH3_kSecret;
		XXH_ALIGN(64) __m256i* dest = (__m256i*)customSecret;

#if defined(__GNUC__) || defined(__clang__)
		/*
		 * On GCC & Clang, marking 'dest' as modified will cause the compiler:
		 *   - do not extract the secret from sse registers in the internal loop
		 *   - use less common registers, and avoid pushing these reg into stack
		 * The asm hack causes Clang to assume that XXH3_kSecretPtr aliases with
		 * customSecret, and on aarch64, this prevented LDP from merging two
		 * loads together for free. Putting the loads together before the stores
		 * properly generates LDP.
		 */
		__asm__("" : "+r"(dest));
#endif

		/* GCC -O2 need unroll loop manually */
		dest[0] = _mm256_add_epi64(_mm256_stream_load_si256(src + 0), seed);
		dest[1] = _mm256_add_epi64(_mm256_stream_load_si256(src + 1), seed);
		dest[2] = _mm256_add_epi64(_mm256_stream_load_si256(src + 2), seed);
		dest[3] = _mm256_add_epi64(_mm256_stream_load_si256(src + 3), seed);
		dest[4] = _mm256_add_epi64(_mm256_stream_load_si256(src + 4), seed);
		dest[5] = _mm256_add_epi64(_mm256_stream_load_si256(src + 5), seed);
	}
}

void XXH3_accumulate_512_sse2(void* XXH_RESTRICT acc, const void* XXH_RESTRICT input, const void* XXH_RESTRICT secret) {
	/* SSE2 is just a half-scale version of the AVX2 version. */
	XXH_ASSERT((((size_t)acc) & 15) == 0);
	{
		XXH_ALIGN(16) __m128i* const xacc = (__m128i*)acc;
		/* Unaligned. This is mainly for pointer arithmetic, and because
		 * _mm_loadu_si128 requires a const __m128i * pointer for some reason. */
		const __m128i* const xinput = (const __m128i*)input;
		/* Unaligned. This is mainly for pointer arithmetic, and because
		 * _mm_loadu_si128 requires a const __m128i * pointer for some reason. */
		const __m128i* const xsecret = (const __m128i*)secret;

		size_t i;
		for (i = 0; i < XXH_STRIPE_LEN / sizeof(__m128i); i++) {
			/* data_vec    = xinput[i]; */
			__m128i const data_vec = _mm_loadu_si128(xinput + i);
			/* key_vec     = xsecret[i]; */
			__m128i const key_vec = _mm_loadu_si128(xsecret + i);
			/* data_key    = data_vec ^ key_vec; */
			__m128i const data_key = _mm_xor_si128(data_vec, key_vec);
			/* data_key_lo = data_key >> 32; */
			__m128i const data_key_lo = _mm_shuffle_epi32(data_key, _MM_SHUFFLE(0, 3, 0, 1));
			/* product     = (data_key & 0xffffffff) * (data_key_lo & 0xffffffff); */
			__m128i const product = _mm_mul_epu32(data_key, data_key_lo);
			/* xacc[i] += swap(data_vec); */
			__m128i const data_swap = _mm_shuffle_epi32(data_vec, _MM_SHUFFLE(1, 0, 3, 2));
			__m128i const sum = _mm_add_epi64(xacc[i], data_swap);
			/* xacc[i] += product; */
			xacc[i] = _mm_add_epi64(product, sum);
		}
	}
}

void XXH3_scrambleAcc_sse2(void* XXH_RESTRICT acc, const void* XXH_RESTRICT secret) {
	XXH_ASSERT((((size_t)acc) & 15) == 0);
	{
		XXH_ALIGN(16) __m128i* const xacc = (__m128i*)acc;
		/* Unaligned. This is mainly for pointer arithmetic, and because
		 * _mm_loadu_si128 requires a const __m128i * pointer for some reason. */
		const __m128i* const xsecret = (const __m128i*)secret;
		const __m128i prime32 = _mm_set1_epi32((int)XXH_PRIME32_1);

		size_t i;
		for (i = 0; i < XXH_STRIPE_LEN / sizeof(__m128i); i++) {
			/* xacc[i] ^= (xacc[i] >> 47) */
			__m128i const acc_vec = xacc[i];
			__m128i const shifted = _mm_srli_epi64(acc_vec, 47);
			__m128i const data_vec = _mm_xor_si128(acc_vec, shifted);
			/* xacc[i] ^= xsecret[i]; */
			__m128i const key_vec = _mm_loadu_si128(xsecret + i);
			__m128i const data_key = _mm_xor_si128(data_vec, key_vec);

			/* xacc[i] *= XXH_PRIME32_1; */
			__m128i const data_key_hi = _mm_shuffle_epi32(data_key, _MM_SHUFFLE(0, 3, 0, 1));
			__m128i const prod_lo = _mm_mul_epu32(data_key, prime32);
			__m128i const prod_hi = _mm_mul_epu32(data_key_hi, prime32);
			xacc[i] = _mm_add_epi64(prod_lo, _mm_slli_epi64(prod_hi, 32));
		}
	}
}

void XXH3_initCustomSecret_sse2(void* XXH_RESTRICT customSecret, xxh_u64 seed64) {
	XXH_STATIC_ASSERT((XXH_SECRET_DEFAULT_SIZE & 15) == 0);
	(void)(&XXH_writeLE64);
	{
		int const nbRounds = XXH_SECRET_DEFAULT_SIZE / sizeof(__m128i);

#if defined(_MSC_VER) && defined(_M_IX86) && _MSC_VER < 1900
		// MSVC 32bit mode does not support _mm_set_epi64x before 2015
		XXH_ALIGN(16) const xxh_i64 seed64x2[2] = { (xxh_i64)seed64, -(xxh_i64)seed64 };
		__m128i const seed = _mm_load_si128((__m128i const*)seed64x2);
#else
		__m128i const seed = _mm_set_epi64x(-(xxh_i64)seed64, (xxh_i64)seed64);
#endif
		int i;

		XXH_ALIGN(64) const float* const src = (float const*)XXH3_kSecret;
		XXH_ALIGN(XXH_SEC_ALIGN) __m128i* dest = (__m128i*)customSecret;
#if defined(__GNUC__) || defined(__clang__)
		/*
		 * On GCC & Clang, marking 'dest' as modified will cause the compiler:
		 *   - do not extract the secret from sse registers in the internal loop
		 *   - use less common registers, and avoid pushing these reg into stack
		 */
		__asm__("" : "+r"(dest));
#endif

		for (i = 0; i < nbRounds; ++i) {
			dest[i] = _mm_add_epi64(_mm_castps_si128(_mm_load_ps(src + i * 4)), seed);
		}
	}
}

// CPU feature detection and initialization
void XXH3_init_cpu_features(void) {
	static int initialized = 0;
	if (initialized)
		return;

	if (cpu_has_avx512()) {
		XXH3_accumulate_512 = XXH3_accumulate_512_avx512;
		XXH3_scrambleAcc = XXH3_scrambleAcc_avx512;
		XXH3_initCustomSecret = XXH3_initCustomSecret_avx512;
	} else if (cpu_has_avx2()) {
		XXH3_accumulate_512 = XXH3_accumulate_512_avx2;
		XXH3_scrambleAcc = XXH3_scrambleAcc_avx2;
		XXH3_initCustomSecret = XXH3_initCustomSecret_avx2;
	} else if (cpu_has_sse2()) {
		XXH3_accumulate_512 = XXH3_accumulate_512_sse2;
		XXH3_scrambleAcc = XXH3_scrambleAcc_sse2;
		XXH3_initCustomSecret = XXH3_initCustomSecret_sse2;
	} else {
		XXH3_accumulate_512 = XXH3_accumulate_512_scalar;
		XXH3_scrambleAcc = XXH3_scrambleAcc_scalar;
		XXH3_initCustomSecret = XXH3_initCustomSecret_scalar;
	}

	initialized = 1;
}
