// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use tracing::debug;
use crate::execute::{AfterExecuteHook, BeforeExecuteHook, get_this_mut, Noop, validate_num_args};
use crate::plugin::Plugin;

#[derive(Default, Clone)]
pub struct RdKafkaPlugin;

impl Plugin for RdKafkaPlugin {
    fn class_names(&self) -> Option<&'static [&'static str]> {
        None
    }

    fn function_name_prefix(&self) -> Option<&'static str> {
        None
    }

    fn hook(
        &self, class_name: Option<&str>, function_name: &str,
    ) -> Option<(
        Box<BeforeExecuteHook>,
        Box<AfterExecuteHook>
    )> {
        debug!("my|class_name = {:?}, function_name = {:?}", class_name, function_name);
        match (class_name, function_name) {
            (
                Some(class_name @ "RdKafka\\Producer"), "produce",
            ) => Some(self.hook_produce()),
            _ => None,
        }
    }
}

impl RdKafkaPlugin {
    fn hook_produce(&self) -> (Box<BeforeExecuteHook>, Box<AfterExecuteHook>) {
        (
            Box::new(|request_id, execute_data| {
                validate_num_args(execute_data, 1)?;
                let this = get_this_mut(execute_data)?;
                debug!("my|RdKafkaPlugin|this = {:?}", this);
                Ok(Box::new(()))
            }),
            Noop::noop(),
        )
    }
}