//! Stream-based consumer implementation.
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
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Context;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tokio_executor::blocking::{run as block_on};


enum MessageStreamState {
    Ready,
    Polling()
}

/// A Kafka consumer implementing Stream.
///
/// It can be used to receive messages as they are consumed from Kafka. Note: there might be
/// buffering between the actual Kafka consumer and the receiving end of this stream, so it is not
/// advised to use automatic commit, as some messages might have been consumed by the internal Kafka
/// consumer but not processed. Manual offset storing should be used, see the `store_offset`
/// function on `Consumer`.
#[pin_project]
pub struct MessageStream<'a, C: ConsumerContext + 'static> {
    consumer: &'a StreamConsumer<C>,
    #[pin]
    receiver: mpsc::Receiver<Option<PolledMessagePtr>>,
}

impl<'a, C: ConsumerContext + 'static> MessageStream<'a, C> {
    fn new(
        consumer: &'a StreamConsumer<C>,
        receiver: mpsc::Receiver<Option<PolledMessagePtr>>,
    ) -> MessageStream<'a, C> {
        MessageStream { consumer, receiver }
    }
}

impl<'a, C: ConsumerContext + 'a> Stream for MessageStream<'a, C> {
    type Item = KafkaResult<BorrowedMessage<'a>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let ready = futures::ready!(this.receiver.poll_next(cx));
        ready.map(|polled_ptr_opt: Option<PolledMessagePtr>| {
            polled_ptr_opt.map_or(
                Err(KafkaError::NoMessageReceived),
                |polled_ptr: PolledMessagePtr| polled_ptr.into_message_of(self.consumer),
            )
        })
    }
}

enum PollLoopResult<'a> {
    Continue,
    Sent(futures::sink::Send<'a, mpsc::Sender<Option<PolledMessagePtr>>, Option<PolledMessagePtr>>),
}
/// Internal consumer loop. This is the main body of the thread that will drive the stream consumer.
/// If `send_none` is true, the loop will send a None into the sender every time the poll times out.
async fn poll_loop<C: ConsumerContext + 'static>(
    consumer: Arc<BaseConsumer<C>>,
    sender: mpsc::Sender<Option<PolledMessagePtr>>,
    should_stop: &AtomicBool,
    poll_interval: Duration,
    send_none: bool,
) {
    trace!("Polling thread loop started");
    let mut curr_sender = sender;
    let poll_interval_ms = duration_to_millis(poll_interval) as i32;
    while !should_stop.load(Ordering::Relaxed) {
        trace!("Polling base consumer");
        let poll_consumer = Arc::clone(&consumer);
        let future_send_future = block_on(move || {
            match poll_consumer.poll_raw(poll_interval_ms) {
                None => {
                    if send_none {
                        PollLoopResult::Sent(curr_sender.send(None))
                    } else {
                        PollLoopResult::Continue
                    }
                }
                Some(m_ptr) => {
                    PollLoopResult::Sent(curr_sender.send(Some(PolledMessagePtr::new(m_ptr))))
                },
            }
        });
        if let PollLoopResult::Sent(f) = future_send_future.await {
            match f.await {
                Ok(_) => {},
                Err(e) => {
                    debug!("Sender not available: {:?}", e);
                    break;
                }
            }
        }
    }
    trace!("Polling thread loop terminated");
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
    consumer: Arc<BaseConsumer<C>>,
    should_stop: Arc<AtomicBool>,
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
        MessageStream::new(self, poll_interval, no_message_error)
    }

    /// Stops the StreamConsumer, blocking the caller until the internal consumer has been stopped.
    pub fn stop(&self) {
        let mut handle = self.handle.lock().unwrap();
        if let Some(handle) = handle.take() {
            trace!("Stopping polling");
            self.should_stop.store(true, Ordering::Relaxed);
            match handle.join() {
                Ok(()) => trace!("Polling stopped"),
                Err(e) => warn!("Failure while terminating thread: {:?}", e),
            };
        }
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

impl<C: ConsumerContext>
