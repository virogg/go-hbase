// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

/**
 * On-wire layout constants for the go-hbase IPC frame format.
 *
 * <p>Every chunk frame on the wire is:
 *
 * <pre>{@code
 * [u32 len][u8 type][u64 req_id][u32 region][u8 hook_id]
 * [u8 chunk_flags][u32 chunk_idx][u32 chunk_total][bytes pb]
 * }</pre>
 *
 * <p>All integers are big-endian. {@code len} is the number of bytes that follow the length field
 * itself ({@link #HEADER_SIZE} + payload). This class mirrors {@code internal/wire/frame.go}.
 */
public final class WireFormat {

  /** Fixed framing header bytes following the u32 length prefix. */
  public static final int HEADER_SIZE = 23;

  /** Total on-wire size cap for a single chunk frame (length prefix + header + payload). */
  public static final int MAX_FRAME_SIZE = 64 * 1024;

  /** Maximum payload bytes carryable in a single chunk. */
  public static final int MAX_PAYLOAD_BYTES = MAX_FRAME_SIZE - 4 - HEADER_SIZE;

  /**
   * Maximum {@code chunk_total} a peer may declare for one message. {@code chunk_total} is a raw,
   * peer-controlled u32 that sizes the reassembly chunk array, so it must be bounded before any
   * allocation keyed off it; 1024 chunks × {@link #MAX_PAYLOAD_BYTES} ≈ 64 MiB, far above any
   * legitimate hook message. Mirrors {@code wire.MaxChunks}.
   */
  public static final int MAX_CHUNKS = 1024;

  /**
   * Maximum concurrent in-progress multi-chunk reassemblies. Abandoned req_ids (final chunk never
   * arrives) must not grow the pending map without bound. Mirrors {@code
   * wire.MaxPendingReassemblies}.
   */
  public static final int MAX_PENDING_REASSEMBLIES = 4096;

  /**
   * Maximum TOTAL payload bytes retained across all in-progress reassemblies. The entry-count cap
   * alone is not enough: each abandoned near-complete reassembly may hold up to (MAX_CHUNKS−1) ×
   * {@link #MAX_PAYLOAD_BYTES} ≈ 67 MB, so 4096 entries would permit ~256 GiB of retained heap — an
   * OOM long before the count cap fires. One max-size message (64 MiB) plus headroom. Mirrors
   * {@code wire.MaxPendingBytes}.
   */
  public static final int MAX_PENDING_BYTES = 96 << 20;

  private WireFormat() {}
}
