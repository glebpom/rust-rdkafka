use crate::rdsys;
use crate::rdsys::types::*;
use futures::channel::mpsc;
use futures::{Poll, SinkExt, Stream, StreamExt};

use crate::config::{ClientConfig, FromClientConfig, FromClientConfigAndContext};
use crate::consumer::base_consumer::BaseConsumer;
use crate::consumer::{Consumer, ConsumerContext, DefaultConsumerContext};
use crate::error::{KafkaError, KafkaResult};
use crate::message::BorrowedMessage;
use crate::util::duration_to_millis;

use log::*;
use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tokio_executor::blocking::{run as block_on};

/// A small wrapper for a message pointer. This wrapper is only used to
/// pass a message between the polling thread and the thread consuming the stream,
/// and to transform it from pointer to `BorrowedMessage` with a lifetime that derives from the
/// lifetime of the stream consumer. In general is not safe to pass a struct with an internal
/// reference across threads. However the `StreamConsumer` guarantees that the polling thread
/// is terminated before the consumer is actually dropped, ensuring that the messages
/// are safe to be used for their entire lifetime.
struct PolledMessagePtr {
    message_ptr: *mut RDKafkaMessage,
}

impl PolledMessagePtr {
    /// Creates a new PolledPtr from a message pointer. It takes the ownership of the message.
    fn new(message_ptr: *mut RDKafkaMessage) -> PolledMessagePtr {
        trace!("New polled ptr {:?}", message_ptr);
        PolledMessagePtr { message_ptr }
    }

    /// Transforms the `PolledMessagePtr` into a message whose lifetime will be bound to the
    /// lifetime of the provided consumer. If the librdkafka message represents an error, the error
    /// will be returned instead.
    fn into_message_of<C: ConsumerContext>(
        mut self,
        consumer: &StreamConsumer<C>,
    ) -> KafkaResult<BorrowedMessage> {
        let msg = unsafe { BorrowedMessage::from_consumer(self.message_ptr, consumer) };
        self.message_ptr = ptr::null_mut();
        msg
    }
}

impl Drop for PolledMessagePtr {
    /// If the `PolledMessagePtr` is hasn't been transformed into a message and the pointer is
    /// still available, it will free the underlying resources.
    fn drop(&mut self) {
        if !self.message_ptr.is_null() {
            trace!("Destroy PolledPtr {:?}", self.message_ptr);
            unsafe { rdsys::rd_kafka_message_destroy(self.message_ptr) };
        }
    }
}

/// Allow message pointer to be moved across threads.
unsafe impl Send for PolledMessagePtr {}


#[pin_project]
pub struct MessageFuture {
    pending: Option<Pin<Box<dyn Future<Output=Result<PolledMessagePtr, Error>>>>>;
}

impl Future for MessageFuture {

}