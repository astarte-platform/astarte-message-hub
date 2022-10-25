/*
 * This file is part of Astarte.
 *
 * Copyright 2022 SECO Mind Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

use std::io::Error;

use async_trait::async_trait;
use tokio::sync::mpsc::Receiver;
use tonic::Status;

use crate::astarte_message_hub::AstarteNode;
use crate::proto_message_hub::AstarteMessage;

#[async_trait]
pub trait AstartePublisher: Send + Sync {
    async fn publish(&self, astarte_message: AstarteMessage) -> Result<(), Error>;
}

#[async_trait]
pub trait AstarteSubscriber {
    async fn subscribe(
        &self,
        astarte_node: &AstarteNode,
    ) -> Result<Receiver<Result<AstarteMessage, Status>>, Error>;
    async fn unsubscribe(&self, astarte_node: &AstarteNode) -> Result<(), Error>;
}
