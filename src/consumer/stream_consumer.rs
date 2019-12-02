//! Stream-based consumer implementation.
use std::future::Future;
use std::pin::Pin;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use log::*;
use tokio::task;

use pin_project::pin_project;

use crate::config::{ClientConfig, FromClientConfig, FromClientConfigAndContext};
use crate::consumer::{Consumer, ConsumerContext, DefaultConsumerContext};
use crate::consumer::base_consumer::BaseConsumer;
use crate::error::KafkaError;
use crate::error::KafkaResult;
use crate::message::BorrowedMessage;
use crate::rdsys;
use crate::rdsys::types::*;
use crate::util::Timeout;

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
    fn into_message<'a>(
        mut self,
    ) -> KafkaResult<BorrowedMessage<'a>> {
        let msg = unsafe { BorrowedMessage::from_consumer(self.message_ptr) };
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

// A Kafka consumer implementing Stream.
///
/// It can be used to receive messages as they are consumed from Kafka. Note: there might be
/// buffering between the actual Kafka consumer and the receiving end of this stream, so it is not
/// advised to use automatic commit, as some messages might have been consumed by the internal Kafka
/// consumer but not processed. Manual offset storing should be used, see the `store_offset`
/// function on `Consumer`.
#[pin_project]
pub struct MessageStream<'a, C: ConsumerContext + 'static> {
    consumer: Arc<BaseConsumer<C>>,
    should_stop: Arc<AtomicBool>,
    poll_interval: Timeout,
    send_none: bool,
    #[pin]
    pending: Option<task::JoinHandle<PollConsumerResult>>,
    phantom: &'a std::marker::PhantomData<C>,
}

enum PollConsumerResult {
    Continue,
    Ready(Option<PolledMessagePtr>),
}

impl<'a, C: ConsumerContext + 'static> MessageStream<'a, C> {
    /// Create a new message stream from a base consumer
    pub fn new(
        consumer: Arc<BaseConsumer<C>>,
        should_stop: Arc<AtomicBool>,
        poll_interval: Duration,
        send_none: bool,
    ) -> Self {
        Self {
            consumer: consumer,
            should_stop: should_stop,
            poll_interval: Timeout::After(poll_interval),
            send_none: send_none,
            pending: None,
            phantom: &std::marker::PhantomData,
        }
    }

    /// Close the message stream
    pub async fn close(&self) {
        self.should_stop.store(true, Ordering::Relaxed);
    }
}

impl<'a, C: ConsumerContext + 'a> Stream for MessageStream<'a, C> {
    type Item = KafkaResult<BorrowedMessage<'a>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let mut pending: Pin<&mut Option<task::JoinHandle<PollConsumerResult>>> = this.pending.as_mut();
        println!("Polling stream for next message");
        loop {
            if this.should_stop.load(Ordering::Relaxed) {
                println!("Stopping");
                return Poll::Ready(None);
            } else {
                if let Some(to_poll) = pending.as_mut().as_pin_mut() {
                    println!("Seeing if poll result is ready");
                    if let PollConsumerResult::Ready(res) = futures::ready!(to_poll.poll(cx)).expect("poll consumer error") {
                        println!("Poll result ready");
                        pending.set(None);
                        let ret = match res {
                            None => {
                                println!("No message polled, but forwarding none as requested");
                                Poll::Ready(Some(Err(KafkaError::NoMessageReceived)))
                            },
                            Some(polled_ptr) => Poll::Ready(Some(polled_ptr.into_message())),
                        };
                        return ret;
                    }
                }
                println!("Requesting next poll result");
                let consumer = Arc::clone(&this.consumer);
                let poll_interval = *this.poll_interval;
                let send_none = *this.send_none;
                let f = task::spawn_blocking(move || {
                    match consumer.poll_raw(poll_interval) {
                        None => {
                            if send_none {
                                PollConsumerResult::Ready(None)
                            } else {
                                PollConsumerResult::Continue
                            }
                        }
                        Some(m_ptr) => {
                            PollConsumerResult::Ready(Some(PolledMessagePtr::new(m_ptr)))
                        },
                    }
                });
                pending.replace(f);
            }
        }
    }
}



/// A Kafka Consumer providing a `futures::Stream` interface.
///
/// This consumer doesn't need to be polled since it has a separate polling thread. Due to the
/// asynchronous nature of the stream, some messages might be consumed by the consumer without being
/// processed on the other end of the stream. If auto commit is used, it might cause message loss
/// after consumer restart. Manual offset storing should be used, see the `store_offset` function on
/// `Consumer`.
#[must_use = "Consumer polling thread will stop immediately if unused"]
pub struct StreamConsumer<C: ConsumerContext + 'static = DefaultConsumerContext> {
    pub (crate) consumer: Arc<BaseConsumer<C>>,
    pub (crate) should_stop: Arc<AtomicBool>,
}

impl<C: ConsumerContext> StreamConsumer<C> {
    /// Expose the underlying consumer
    pub fn consumer(&self) -> Arc<BaseConsumer<C>> {
        self.consumer.clone()
    }
}

impl<C: ConsumerContext> Consumer<C> for StreamConsumer<C> {
    fn get_base_consumer(&self) -> &BaseConsumer<C> {
        Arc::as_ref(&self.consumer)
    }
}

impl FromClientConfig for StreamConsumer {
    fn from_config(config: &ClientConfig) -> KafkaResult<StreamConsumer> {
        StreamConsumer::from_config_and_context(config, DefaultConsumerContext)
    }
}

/// Creates a new `StreamConsumer` starting from a `ClientConfig`.
impl<C: ConsumerContext> FromClientConfigAndContext<C> for StreamConsumer<C> {
    fn from_config_and_context(
        config: &ClientConfig,
        context: C,
    ) -> KafkaResult<StreamConsumer<C>> {
        let stream_consumer = StreamConsumer {
            consumer: Arc::new(BaseConsumer::from_config_and_context(config, context)?),
            should_stop: Arc::new(AtomicBool::new(false)),
        };
        Ok(stream_consumer)
    }
}

impl<C: ConsumerContext> StreamConsumer<C> {
    /// Starts the StreamConsumer with default configuration (100ms polling interval and no
    /// `NoMessageReceived` notifications).
    pub fn start(&self) -> MessageStream<C> {
        self.start_with(Duration::from_millis(100), false)
    }

    /// Starts the StreamConsumer with the specified poll interval. Additionally, if
    /// `no_message_error` is set to true, it will return an error of type
    /// `KafkaError::NoMessageReceived` every time the poll interval is reached and no message has
    /// been received.
    pub fn start_with(&self, poll_interval: Duration, no_message_error: bool) -> MessageStream<C> {
        MessageStream::new(Arc::clone(&self.consumer), Arc::clone(&self.should_stop), poll_interval, no_message_error)
    }

    /// Stops the StreamConsumer
    pub fn stop(&self) {
        self.should_stop.store(true, Ordering::Relaxed);
    }
}

impl<C: ConsumerContext> Drop for StreamConsumer<C> {
    fn drop(&mut self) {
        trace!("Destroy StreamConsumer");
        // The polling thread must be fully stopped before we can proceed with the actual drop,
        // otherwise it might consume from a destroyed consumer.
        self.stop();
    }
}
