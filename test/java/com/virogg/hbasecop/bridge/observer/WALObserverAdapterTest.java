// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.virogg.hbasecop.bridge.config.PolicyConfig;
import com.virogg.hbasecop.bridge.wire.pb.HookError;
import com.virogg.hbasecop.bridge.wire.pb.HookResponse;
import com.virogg.hbasecop.bridge.wire.pb.PostWALRollRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreWALWriteRequest;
import java.io.IOException;
import java.time.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessorEnvironment;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * T53 Wave B unit test for {@link WALObserverAdapter}: verifies the adapter encodes the WALKey
 * scalars / WAL-roll paths into the right proto Request, drives the injected {@link
 * HookDispatcher}, and translates {@code bypass=true} / strict-mode error responses into the
 * matching {@code ObserverContext#bypass()} / {@code IOException} reactions - mirrors the master
 * adapter's coverage on the WAL surface.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class WALObserverAdapterTest {

  @Mock private HookDispatcher dispatcher;
  @Mock private ObserverContext<WALCoprocessorEnvironment> ctx;
  @Mock private WALKey logKey;

  private WALObserverAdapter adapter;

  @BeforeEach
  void setUp() {
    adapter = new WALObserverAdapter(dispatcher, new PolicyConfig(new Configuration(false)));
  }

  @Test
  void preWALWrite_encodesSeqNumAndDispatches()
      throws IOException, InterruptedException, java.util.concurrent.TimeoutException {
    when(logKey.getSequenceId()).thenReturn(987L);
    when(logKey.getWriteTime()).thenReturn(1_700_000_000_000L);
    when(logKey.getTableName()).thenReturn(TableName.valueOf("default", "t"));
    when(dispatcher.dispatchHook(
            anyInt(), eq(HookId.PRE_WAL_WRITE.value()), any(), any(Duration.class)))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());

    adapter.preWALWrite(ctx, null, logKey, null);

    ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
    verify(dispatcher, times(1))
        .dispatchHook(
            anyInt(), eq(HookId.PRE_WAL_WRITE.value()), payload.capture(), any(Duration.class));
    PreWALWriteRequest req = parseWrite(payload.getValue());
    assertEquals(987L, req.getLogKey().getLogSeqNum(), "log seq num not round-tripped");
    verify(ctx, times(0)).bypass();
  }

  @Test
  void preWALWrite_bypassPropagates()
      throws IOException, InterruptedException, java.util.concurrent.TimeoutException {
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any(Duration.class)))
        .thenReturn(HookResponse.newBuilder().setBypass(true).build().toByteArray());

    adapter.preWALWrite(ctx, null, null, null);

    verify(ctx, times(1)).bypass();
  }

  @Test
  void preWALWrite_strictErrorResponseRaisesIOException()
      throws IOException, InterruptedException, java.util.concurrent.TimeoutException {
    Configuration conf = new Configuration(false);
    conf.set("hbasecop.policy.preWALWrite", "strict");
    adapter = new WALObserverAdapter(dispatcher, new PolicyConfig(conf));

    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any(Duration.class)))
        .thenReturn(
            HookResponse.newBuilder()
                .setError(HookError.newBuilder().setCode(7).setMessage("wal vetoed"))
                .build()
                .toByteArray());

    IOException thrown =
        assertThrows(IOException.class, () -> adapter.preWALWrite(ctx, null, null, null));
    assertTrue(
        thrown.getMessage().contains("wal vetoed"),
        "strict failure must surface the observer message; got: " + thrown.getMessage());
    verify(ctx, times(0)).bypass();
  }

  @Test
  void postWALRoll_encodesPathsAndDispatches()
      throws IOException, InterruptedException, java.util.concurrent.TimeoutException {
    when(dispatcher.dispatchHook(
            anyInt(), eq(HookId.POST_WAL_ROLL.value()), any(), any(Duration.class)))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());

    adapter.postWALRoll(ctx, new Path("/wal/old.1"), new Path("/wal/new.2"));

    ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
    verify(dispatcher, times(1))
        .dispatchHook(
            anyInt(), eq(HookId.POST_WAL_ROLL.value()), payload.capture(), any(Duration.class));
    PostWALRollRequest req = parseRoll(payload.getValue());
    assertTrue(req.getOldPath().endsWith("/wal/old.1"), "old path not round-tripped");
    assertTrue(req.getNewPath().endsWith("/wal/new.2"), "new path not round-tripped");
  }

  // --- helpers -------------------------------------------------------------

  private static PreWALWriteRequest parseWrite(byte[] bytes) {
    try {
      return PreWALWriteRequest.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new AssertionError("parse PreWALWriteRequest", e);
    }
  }

  private static PostWALRollRequest parseRoll(byte[] bytes) {
    try {
      return PostWALRollRequest.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new AssertionError("parse PostWALRollRequest", e);
    }
  }
}
