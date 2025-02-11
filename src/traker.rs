use std::{
    future::Future,
    time::{Duration, Instant},
};

use pin_project::pin_project;

#[derive(Debug)]
pub struct ExecutionInfoTrack {
    begin_time: Instant,
    pub process_time: Duration,
}

impl ExecutionInfoTrack {
    pub fn new() -> Self {
        Self {
            begin_time: Instant::now(),
            process_time: Duration::default(),
        }
    }

    pub fn on_begin(&mut self) {
        self.begin_time = Instant::now();
    }

    pub fn on_finish(&mut self) {
        self.process_time += self.begin_time.elapsed();
    }
}

pub fn track<'a, F: Future + 'a>(
    f: F,
    track: &'a mut ExecutionInfoTrack,
) -> impl Future<Output = F::Output> + 'a {
    Tracker { fut: f, track }
}

#[pin_project]
pub struct Tracker<'a, F: Future> {
    #[pin]
    fut: F,
    track: &'a mut ExecutionInfoTrack,
}

impl<'a, F> Future for Tracker<'a, F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut this = self.project();
        this.track.on_begin();
        let res = this.fut.poll(cx);
        this.track.on_finish();
        res
    }
}
