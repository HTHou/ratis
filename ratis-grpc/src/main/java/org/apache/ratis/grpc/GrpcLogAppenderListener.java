/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.grpc;

import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;

/**
 * Observes a single peer log appender. Callbacks run on Ratis threads, may be concurrent and may
 * hold an appender lock. They must not block or retain request payloads. Exceptions are isolated
 * from replication. Callbacks describe lifecycle activity, not application-level outcomes:
 * consumers are responsible for correlating requests, handling racing terminal notifications,
 * filtering messages and aggregating snapshot chunks.
 * Append request, matched-reply and reset callbacks are serialized per appender. Failure
 * callbacks may race with these callbacks. No exactly-once terminal notification is guaranteed.
 */
public interface GrpcLogAppenderListener {
  /** Creates a separate listener for each appender, including after leadership changes. */
  @FunctionalInterface
  interface Factory {
    /** @return the listener, or null to disable observation for this appender. */
    GrpcLogAppenderListener create(RaftGroupMemberId source, RaftPeer destination);
  }

  /** An append attempt is registered, before establishing or writing its stream. */
  default void onAppendEntriesRequest(AppendEntriesRequestProto request) { }

  /** A response was matched to a pending append request. */
  default void onAppendEntriesReply(AppendEntriesReplyProto reply) { }

  /** A local send error or request timeout occurred; a later stream notification may follow. */
  default void onAppendEntriesFailure(long callId, Throwable error) { }

  /** Pending append attempts are invalidated by a reset or by a stopped response stream. */
  default void onAppendEntriesReset(Throwable error) { }

  /** A snapshot attempt starts before stream creation, including notification-only attempts. */
  default void onInstallSnapshotStart(String requestId, boolean notificationOnly) { }

  /** A snapshot chunk or notification is about to be sent. */
  default void onInstallSnapshotRequest(String requestId, InstallSnapshotRequestProto request) { }

  /** A snapshot response was received; this is not necessarily a terminal response. */
  default void onInstallSnapshotReply(String requestId, InstallSnapshotReplyProto reply) { }

  /**
   * The stream completed (null error), failed, or its sender was interrupted. Completion alone
   * does not imply that all chunks were sent or acknowledged. Racing notifications may repeat.
   */
  default void onInstallSnapshotEnd(String requestId, Throwable error) { }
}
