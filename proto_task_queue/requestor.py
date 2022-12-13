# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Client code for requesting tasks over Cloud Pub/Sub."""

from concurrent import futures
import logging
from typing import Callable, Optional

from proto_task_queue import task_pb2

from google.cloud.pubsub_v1.publisher import client
from google.protobuf import message
from google.protobuf import text_format


class Requestor(object):
  """Client for sending tasks to background workers over Cloud Pub/Sub."""

  _publisher: client.Client

  def __init__(
      self,
      pubsub_publisher_client: Optional[client.Client] = None,
      *,
      task_to_string: Callable[[task_pb2.Task],
                               str] = text_format.MessageToString,
  ):
    """Constructor.

    Args:
      pubsub_publisher_client: Cloud Pub/Sub publisher client, or None to use
        the default.
      task_to_string: Function that converts a Task to a human-readable string
        for logging.
    """
    self._publisher = pubsub_publisher_client or client.Client()
    self._task_to_string = task_to_string

  def request(self, topic: str, args: message.Message) -> futures.Future:
    """Constructs a Task proto and sends it to background workers.

    Most callers should use this method unless they have a reason to construct
    the Task proto themselves.

    Args:
      topic: Resource name of the pubsub topic to send the request to.
      args: Task arguments. The type of this proto determines which task to
        call.

    Returns:
      Future for the request. The future will complete when the request is sent,
      not when the task is completed.
    """
    task = task_pb2.Task()
    task.args.Pack(args)
    return self.request_task(topic, task)

  def request_task(self, topic: str, task: task_pb2.Task) -> futures.Future:
    """Sends a Task proto to background workers.

    Prefer using request() above if you don't already have a Task proto.

    Args:
      topic: Resource name of the pubsub topic to send the request to.
      task: Task to send.

    Returns:
      Future for the request. The future will complete when the request is sent,
      not when the task is completed.
    """
    task_bytes = task.SerializeToString()
    logging.info('Sending background task to %s:\n%s', topic,
                 self._task_to_string(task))
    return self._publisher.publish(topic, task_bytes)
