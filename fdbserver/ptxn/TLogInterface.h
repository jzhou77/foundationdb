/*
 * TLogInterface.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FDBSERVER_PTXN_TLOGINTERFACE_H
#define FDBSERVER_PTXN_TLOGINTERFACE_H

#pragma once

#include <memory>
#include <vector>

#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbserver/ptxn/Config.h"
#include "flow/Arena.h"
#include "flow/FileIdentifier.h"

namespace ptxn {

struct TLogCommitReply {
	constexpr static FileIdentifier file_identifier = 178491;

	Version version;

	TLogCommitReply() {}
	explicit TLogCommitReply(Version version) : version(version) {}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, version);
	}
};

struct TLogCommitRequest {
	constexpr static FileIdentifier file_identifier = 316371;

	// SpanID for tracing
	SpanID spanID;

	// Team ID
	StorageTeamID storageTeamID;

	// Arena
	Arena arena;

	// Messages (store the mutation data)
	StringRef messages;

	// Versions
	Version prevVersion;
	Version version;
	Version knownCommittedVersion;
	Version minKnownCommittedVersion;

	// Debug ID
	Optional<UID> debugID;

	// Response
	ReplyPromise<TLogCommitReply> reply;

	TLogCommitRequest() = default;
	TLogCommitRequest(const SpanID& spanID_,
	                  const StorageTeamID& storageTeamID_,
	                  const Arena arena_,
	                  StringRef messages_,
	                  const Version prevVersion_,
	                  const Version version_,
	                  const Version knownCommittedVersion_,
	                  const Version minKnownCommittedVersion_,
	                  const Optional<UID>& debugID_)
	  : spanID(spanID_), storageTeamID(storageTeamID_), arena(arena_), messages(messages_), prevVersion(prevVersion_),
	    version(version_), knownCommittedVersion(knownCommittedVersion_),
	    minKnownCommittedVersion(minKnownCommittedVersion_), debugID(debugID_) {}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           spanID,
		           arena,
		           messages,
		           prevVersion,
		           version,
		           knownCommittedVersion,
		           minKnownCommittedVersion,
		           debugID,
		           reply);
	}
};

struct TLogPeekReply {
	static constexpr FileIdentifier file_identifier = 292724;

	Optional<UID> debugID;

	// Arena containing the serialized mutation data, see TLogStorageServerPeekSerializer
	Arena arena;
	// StringRef referring the serialized mutation data, see TLogStorageServerPeekSerializer
	StringRef data;

	Version end;
	Optional<Version> popped;
	Version maxKnownVersion;
	Version minKnownCommittedVersion;
	Optional<Version> begin;
	bool onlySpilled = false;

	TLogPeekReply() = default;
	TLogPeekReply(const Optional<UID>& debugID_, Arena& arena_, StringRef data_)
	  : debugID(debugID_), arena(arena_), data(data_) {}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(
		    ar, debugID, arena, data, end, popped, maxKnownVersion, minKnownCommittedVersion, begin, onlySpilled);
	}
};

struct TLogPeekRequest {
	static constexpr FileIdentifier file_identifier = 356070;

	Optional<UID> debugID;

	Arena arena;
	// We are interested in versions between [beginVersion, endVersion)
	// Following the C++ custom, the endVersion is *EXCLUSIVE*.
	Version beginVersion;
	Optional<Version> endVersion;
	StorageTeamID storageTeamID;

	Tag tag;
	bool returnIfBlocked;
	bool onlySpilled;
	Optional<std::pair<UID, int>> sequence;
	ReplyPromise<TLogPeekReply> reply;

	TLogPeekRequest() = default;
	TLogPeekRequest(const Optional<UID>& debugID_,
	                const Version& beginVersion_,
	                const Version& endVersion_,
	                const StorageTeamID& storageTeamID_)
	  : debugID(debugID_), beginVersion(beginVersion_), endVersion(endVersion_), storageTeamID(storageTeamID_) {}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           debugID,
		           arena,
		           beginVersion,
		           endVersion,
		           storageTeamID,
		           tag,
		           returnIfBlocked,
		           onlySpilled,
		           sequence,
		           reply);
	}
};

struct TLogPopRequest {
	static constexpr FileIdentifier file_identifier = 288041;

	Arena arena;
	Version version;
	Version durableKnownCommittedVersion;
	Tag tag;
	StorageTeamID storageTeamID;

	ReplyPromise<Void> reply;

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, arena, version, durableKnownCommittedVersion, tag, storageTeamID, reply);
	}
};

struct TLogLockResult {
	constexpr static FileIdentifier file_identifier = 5232634;
	Version end;
	Version knownCommittedVersion;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, end, knownCommittedVersion);
	}
};

struct TLogRecoveryFinishedRequest {
	constexpr static FileIdentifier file_identifier = 6634364;
	ReplyPromise<Void> reply;

	TLogRecoveryFinishedRequest() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct TLogConfirmRunningRequest {
	constexpr static FileIdentifier file_identifier = 8013123;
	Optional<UID> debugID;
	ReplyPromise<Void> reply;

	TLogConfirmRunningRequest() {}
	TLogConfirmRunningRequest(Optional<UID> debugID) : debugID(debugID) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, debugID, reply);
	}
};

struct VerUpdateRef {
	Version version;
	VectorRef<MutationRef> mutations;
	bool isPrivateData;

	VerUpdateRef() : isPrivateData(false), version(invalidVersion) {}
	VerUpdateRef(Arena& to, const VerUpdateRef& from)
	  : version(from.version), mutations(to, from.mutations), isPrivateData(from.isPrivateData) {}
	int expectedSize() const { return mutations.expectedSize(); }

	MutationRef push_back_deep(Arena& arena, const MutationRef& m) {
		mutations.push_back_deep(arena, m);
		return mutations.back();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, mutations, isPrivateData);
	}
};

struct TagMessagesRef {
	Tag tag;
	VectorRef<int> messageOffsets;

	TagMessagesRef() {}
	TagMessagesRef(Arena& a, const TagMessagesRef& from) : tag(from.tag), messageOffsets(a, from.messageOffsets) {}

	size_t expectedSize() const { return messageOffsets.expectedSize(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, tag, messageOffsets);
	}
};

struct TLogQueuingMetricsReply {
	constexpr static FileIdentifier file_identifier = 3123450;
	double localTime;
	int64_t instanceID; // changes if bytesDurable and bytesInput reset
	int64_t bytesDurable, bytesInput;
	StorageBytes storageBytes;
	Version v; // committed version

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, localTime, instanceID, bytesDurable, bytesInput, storageBytes, v);
	}
};

struct TLogQueuingMetricsRequest {
	constexpr static FileIdentifier file_identifier = 8756454;
	ReplyPromise<struct TLogQueuingMetricsReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct TLogDisablePopRequest {
	constexpr static FileIdentifier file_identifier = 4252331;
	Arena arena;
	UID snapUID;
	ReplyPromise<Void> reply;
	Optional<UID> debugID;

	TLogDisablePopRequest() = default;
	TLogDisablePopRequest(const UID uid) : snapUID(uid) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, snapUID, reply, arena, debugID);
	}
};

struct TLogEnablePopRequest {
	constexpr static FileIdentifier file_identifier = 9025233;
	Arena arena;
	UID snapUID;
	ReplyPromise<Void> reply;
	Optional<UID> debugID;

	TLogEnablePopRequest() = default;
	TLogEnablePopRequest(const UID uid) : snapUID(uid) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, snapUID, reply, arena, debugID);
	}
};

struct TLogSnapRequest {
	constexpr static FileIdentifier file_identifier = 6454632;
	ReplyPromise<Void> reply;
	Arena arena;
	StringRef snapPayload;
	UID snapUID;
	StringRef role;

	TLogSnapRequest(StringRef snapPayload, UID snapUID, StringRef role)
	  : snapPayload(snapPayload), snapUID(snapUID), role(role) {}
	TLogSnapRequest() = default;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply, snapPayload, snapUID, role, arena);
	}
};

struct TLogInterfaceBase {

	constexpr static FileIdentifier file_identifier = 4121433;

	RequestStream<TLogCommitRequest> commit;
	RequestStream<TLogPeekRequest> peekMessages;
	RequestStream<TLogPopRequest> popMessages;
	RequestStream<ReplyPromise<TLogLockResult>> lock; // first stage of database recovery
	RequestStream<TLogQueuingMetricsRequest> getQueuingMetrics;
	RequestStream<TLogConfirmRunningRequest> confirmRunning; // used for getReadVersion requests from client
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<TLogRecoveryFinishedRequest> recoveryFinished;
	RequestStream<TLogSnapRequest> snapRequest;

	UID id() const { return uniqueID; }
	UID getSharedTLogID() const { return sharedTLogID; }
	std::string toString() const { return id().shortString(); }
	bool operator==(TLogInterfaceBase const& r) const { return id() == r.id(); }
	bool operator!=(const TLogInterfaceBase& r) const { return !this->operator==(r); }
	NetworkAddress address() const { return commit.getEndpoint().getPrimaryAddress(); }
	Optional<NetworkAddress> secondaryAddress() const { return commit.getEndpoint().addresses.secondaryAddress; }

	MessageTransferModel getMessageTransferModel() const;

	void initEndpoints();

protected:
	UID uniqueID;
	UID sharedTLogID;
	MessageTransferModel messageTransferModel;
	LocalityData filteredLocality;

	explicit TLogInterfaceBase(const MessageTransferModel& model_ = MessageTransferModel::StorageServerActivelyPull)
	  : TLogInterfaceBase(LocalityData(), model_) {}

	TLogInterfaceBase(const LocalityData& locality_,
	                  const MessageTransferModel& model_ = MessageTransferModel::StorageServerActivelyPull)
	  : uniqueID(deterministicRandom()->randomUniqueID()), filteredLocality(locality_), sharedTLogID(uniqueID),
	    messageTransferModel(model_) {}

	TLogInterfaceBase(UID sharedTLogID_,
	                  const LocalityData& locality_,
	                  const MessageTransferModel& model_ = MessageTransferModel::StorageServerActivelyPull)
	  : TLogInterfaceBase(deterministicRandom()->randomUniqueID(), sharedTLogID_, locality_, model_) {}

	TLogInterfaceBase(UID id_,
	                  UID sharedTLogID_,
	                  const LocalityData& locality_,
	                  const MessageTransferModel& model_ = MessageTransferModel::StorageServerActivelyPull)
	  : uniqueID(id_), sharedTLogID(sharedTLogID_), filteredLocality(locality_), messageTransferModel(model_) {}

	virtual void initEndpointsImpl(std::vector<ReceiverPriorityPair>&& receivers = {});

	template <typename Ar>
	void serializeImpl(Ar& ar) {
		if constexpr (!is_fb_function<Ar>) {
			ASSERT(ar.isDeserializing || uniqueID != UID());
		}
		serializer(ar, uniqueID, sharedTLogID, filteredLocality, messageTransferModel, commit);
		if (Ar::isDeserializing) {
			peekMessages = RequestStream<TLogPeekRequest>(commit.getEndpoint().getAdjustedEndpoint(1));
			popMessages = RequestStream<TLogPopRequest>(commit.getEndpoint().getAdjustedEndpoint(2));
			lock = RequestStream<ReplyPromise<TLogLockResult>>(commit.getEndpoint().getAdjustedEndpoint(3));
			getQueuingMetrics = RequestStream<TLogQueuingMetricsRequest>(commit.getEndpoint().getAdjustedEndpoint(4));
			confirmRunning = RequestStream<TLogConfirmRunningRequest>(commit.getEndpoint().getAdjustedEndpoint(5));
			waitFailure = RequestStream<ReplyPromise<Void>>(commit.getEndpoint().getAdjustedEndpoint(6));
			recoveryFinished = RequestStream<TLogRecoveryFinishedRequest>(commit.getEndpoint().getAdjustedEndpoint(7));
			snapRequest = RequestStream<TLogSnapRequest>(commit.getEndpoint().getAdjustedEndpoint(8));
		}
	}
};

struct TLogInterface_ActivelyPush : public TLogInterfaceBase {
	constexpr static FileIdentifier file_identifier = 386669;

	template <typename Ar>
	void serialize(Ar& ar) {
		TLogInterfaceBase::serializeImpl(ar);
	}

	TLogInterface_ActivelyPush() : TLogInterfaceBase(MessageTransferModel::TLogActivelyPush) {}

protected:
	virtual void initEndpointsImpl(std::vector<ReceiverPriorityPair>&& receiver = {}) override;
};

struct TLogInterface_PassivelyPull : public TLogInterfaceBase {
	constexpr static FileIdentifier file_identifier = 748550;

	RequestStream<TLogDisablePopRequest> disablePopRequest;
	RequestStream<TLogEnablePopRequest> enablePopRequest;

	template <typename Ar>
	void serialize(Ar& ar) {
		TLogInterfaceBase::serializeImpl(ar);
		serializer(ar, disablePopRequest);
		if (Ar::isDeserializing) {
			enablePopRequest =
			    RequestStream<TLogEnablePopRequest>(disablePopRequest.getEndpoint().getAdjustedEndpoint(1));
		}
	}

	TLogInterface_PassivelyPull()
	  : TLogInterfaceBase(LocalityData(), MessageTransferModel::StorageServerActivelyPull) {}

	explicit TLogInterface_PassivelyPull(const LocalityData& locality_)
	  : TLogInterfaceBase(deterministicRandom()->randomUniqueID(),
	                      locality_,
	                      MessageTransferModel::StorageServerActivelyPull) {}

	TLogInterface_PassivelyPull(const UID& sharedLogId_, const LocalityData& locality_)
	  : TLogInterfaceBase(deterministicRandom()->randomUniqueID(),
	                      sharedLogId_,
	                      locality_,
	                      MessageTransferModel::StorageServerActivelyPull) {}

	TLogInterface_PassivelyPull(const UID& id_, const UID sharedLogId_, const LocalityData& locality_)
	  : TLogInterfaceBase(id_, sharedLogId_, locality_, MessageTransferModel::StorageServerActivelyPull) {}

protected:
	virtual void initEndpointsImpl(std::vector<ReceiverPriorityPair>&& receiver = {}) override;
};

std::shared_ptr<TLogInterfaceBase> getNewTLogInterface(
    const MessageTransferModel model,
    const UID& id = deterministicRandom()->randomUniqueID(),
    const UID& sharedTLogID = deterministicRandom()->randomUniqueID(),
    const LocalityData& locality = LocalityData());

} // namespace ptxn

#endif // FDBSERVER_PTXN_TLOGINTERFACE_H
