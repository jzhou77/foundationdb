/*
 * StorageServerInterface.cpp
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

#include "fdbserver/ptxn/StorageServerInterface.h"

namespace ptxn {

void StorageServerInterfaceBase::initEndpoints() {
	initEndpointsImpl();
}

void StorageServerInterfaceBase::initEndpointsImpl(std::vector<ReceiverPriorityPair>&& receivers) {
	// No additional receivers added in this layer
	if (receivers.size() != 0) {
		FlowTransport::transport().addEndpoints(receivers);
	}
}

void StorageServerInterface_ActivelyPull::initEndpointsImpl(std::vector<ReceiverPriorityPair>&& receivers) {
	// No additional receivers added in this layer
	StorageServerInterfaceBase::initEndpointsImpl(std::move(receivers));
}

void StorageServerInterface_PassivelyReceive::initEndpointsImpl(std::vector<ReceiverPriorityPair>&& receivers) {
	receivers.push_back(pushRequests.getReceiver());
	StorageServerInterfaceBase::initEndpointsImpl(std::move(receivers));
}

std::shared_ptr<StorageServerInterfaceBase> getNewStorageServerInterface(const MessageTransferModel model) {
	switch (model) {
	case MessageTransferModel::TLogActivelyPush:
		return std::make_shared<StorageServerInterface_PassivelyReceive>();
	case MessageTransferModel::StorageServerActivelyPull:
		return std::make_shared<StorageServerInterface_ActivelyPull>();
	default:
		throw internal_error_msg("Unsupported TLog Interface");
	}
}

} // namespace ptxn
