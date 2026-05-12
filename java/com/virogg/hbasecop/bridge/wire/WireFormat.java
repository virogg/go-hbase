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

  private WireFormat() {}
}
