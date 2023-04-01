use anyhow::{anyhow, Result};
use crossbeam::{channel::Receiver, channel::Sender, thread};
use hashbrown::HashMap;
use parking_lot::Mutex;

type ShareSender<T> = Sender<Result<(T, bool)>>;
type ShareReceiver<T> = Receiver<Result<(T, bool)>>;

// call is an in-flight or completed singleflight.go call
struct Call<T>
where
    T: Default + Clone,
{
    dup: usize,

    // ShareSender for the execution call to send work res to other duplicate callers
    // ShareReceiver for duplicate callers to receive execution call's res
    chan: (ShareSender<T>, ShareReceiver<T>),
}

impl<T> Clone for Call<T>
where
    T: Default + Clone,
{
    fn clone(&self) -> Self {
        Self {
            chan: self.chan.clone(),

            dup: self.dup,
        }
    }
}

impl<T> Call<T>
where
    T: Default + Clone,
{
    fn new() -> Call<T> {
        Call {
            chan: crossbeam::channel::unbounded(),
            dup: 0,
        }
    }
}

/// Group represents a class of work and creates a space in which units of work
/// can be executed with duplicate suppression.
pub struct Group<T>
where
    T: Default + Clone + Send,
{
    shared_chans: Mutex<HashMap<String, Call<T>>>,
}

impl<T> Default for Group<T>
where
    T: Default + Clone + Send,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Group<T>
where
    T: Default + Clone + Send,
{
    pub fn new() -> Group<T> {
        Group {
            shared_chans: Mutex::new(HashMap::new()),
        }
    }

    // go executes and returns the results of the given function, making
    // sure that only one execution is in-flight for a given key at a
    // time. If a duplicate comes in, the duplicate caller waits for the
    // original to complete and receives the same results.
    // The bool value indicates whether v was given to multiple callers.
    pub fn go<F>(&self, key: &str, func: F) -> Result<(T, bool)>
    where
        F: Fn() -> Result<T>,
    {
        let mut share = self.shared_chans.lock();

        if let Some(call) = share.get_mut(key) {
            call.dup += 1;
            let call = call.clone();
            drop(share);
            let res = call.chan.1.recv().unwrap();
            return res;
        }

        let call = Call::new();
        share.entry(key.to_string()).or_insert(call);
        drop(share);

        let func_res = func();

        let mut shared = self.shared_chans.lock();
        let call = shared.remove(key).unwrap();
        drop(shared);

        for _ in 0..=call.dup {
            let shared_value = match &func_res {
                Result::Ok(val) => anyhow::Result::Ok((val.clone(), call.dup > 0)),
                Result::Err(err) => Err(anyhow!(err.to_string())),
            };
            call.chan.0.send(shared_value).unwrap();
        }

        call.chan.1.recv().unwrap()
    }

    // DoChan is like Do but returns a channel that will receive the
    // results when they are ready.
    pub fn go_chan<F>(&self, key: &str, func: F) -> ShareReceiver<T>
    where
        F: Fn() -> Result<T>,
        F: Sync,
    {
        let mut share = self.shared_chans.lock();

        if let Some(call) = share.get_mut(key) {
            call.dup += 1;
            let call = call.clone();
            drop(share);
            let (shared_send, shared_recv) = crossbeam::channel::bounded(1);
            thread::scope(|sco| {
                sco.spawn(|_| {
                    shared_send.send(call.chan.1.recv().unwrap()).unwrap();
                });
            })
            .unwrap();
            return shared_recv;
        }

        let call = Call::new();
        share.entry(key.to_string()).or_insert(call);
        drop(share);

        let (s, r): (ShareSender<T>, ShareReceiver<T>) = crossbeam::channel::bounded(1);

        thread::scope(|sco| {
            sco.spawn(|_| {
                let func_res = func();

                let mut shared = self.shared_chans.lock();
                let call = shared.remove(key).unwrap();
                drop(shared);

                for i in 0..=call.dup {
                    let shared_value = match &func_res {
                        Result::Ok(val) => anyhow::Result::Ok((val.clone(), call.dup > 0)),
                        Result::Err(err) => Err(anyhow!(err.to_string())),
                    };
                    if i == call.dup {
                        s.send(shared_value).unwrap();
                    } else {
                        call.chan.0.send(shared_value).unwrap();
                    }
                }
            });
        })
        .unwrap();

        r
    }
}

#[cfg(test)]
mod tests {

    use super::Group;

    const RES: usize = 7;

    #[test]
    fn test_go_simple() {
        let g = Group::new();
        let res = g.go("key", || Ok(RES));
        // simple call's result should not be shared
        assert_eq!(res.unwrap(), (RES, false));
    }

    #[test]
    fn test_go_chan_simple() {
        let g = Group::new();
        let ch = g.go_chan("key", || Ok(RES));
        assert_eq!(ch.capacity().unwrap(), 1);
        // simple call's result should not be shared
        assert_eq!(ch.recv().unwrap().unwrap(), (RES, false));
    }

    #[test]
    fn test_go_multiple_threads() {
        use std::time::Duration;

        use crossbeam::thread;

        fn expensive_fn() -> anyhow::Result<usize> {
            std::thread::sleep(Duration::new(0, 500));
            Ok(RES)
        }

        let g = Group::new();
        thread::scope(|s| {
            for _ in 0..10 {
                s.spawn(|_| {
                    let res = g.go("key", expensive_fn);
                    // mutiple call's result may be shared by ohter duplicate calls
                    assert_eq!(res.unwrap().0, RES);
                });
            }
        })
        .unwrap();
    }

    #[test]
    fn test_go_chan_mutiple_threads() {
        use std::time::Duration;

        use crossbeam::thread;

        fn expensive_fn() -> anyhow::Result<usize> {
            std::thread::sleep(Duration::new(0, 500));
            Ok(RES)
        }

        let g = Group::new();
        thread::scope(|s| {
            for _ in 0..10 {
                s.spawn(|_| {
                    let ch = g.go_chan("key", expensive_fn);
                    assert_eq!(ch.capacity().unwrap(), 1);
                    // mutiple call's result may be shared by ohter duplicate calls
                    assert_eq!(ch.recv().unwrap().unwrap().0, RES);
                });
            }
        })
        .unwrap();
    }
}
