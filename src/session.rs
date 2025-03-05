use tokio::sync::{oneshot, Mutex, OwnedRwLockReadGuard, RwLock, Semaphore};
use tokio::time::Instant;
use std::future::Future;
use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

pub trait Command<S>: Send + Sync + 'static {
    type Ret: Send + Sync + 'static;
    /// IMPORTANT must be deterministic
    fn apply(self, value: &mut S) -> Self::Ret;
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Nonce(pub u64);

#[derive(Serialize, Debug, Clone)]
pub struct SendCommand<'a, C> {
    command: &'a C,
    expected_nonce: Nonce,
    new_nonce: Nonce,
}

pub trait CommandReceiveFn<C>: Send {
    fn receive(&mut self, recv_msg: ReceiveCommand<C>) -> impl Future<Output = ()> + Send;
}

#[derive(Deserialize, Debug, Clone)]
pub struct ReceiveCommand<C> {
    command: C,
    expected_nonce: Nonce,
    new_nonce: Nonce,
}

pub trait DistributedLog<K, C>: Send + Sync + 'static {
    type Err: Send + Sync + 'static;

    fn send(&self, key: &K, msg: SendCommand<C>) -> impl Future<Output = Result<(), Self::Err>> + Send;

    fn receive(&self, key: &K, f: &mut impl CommandReceiveFn<C>) -> impl Future<Output = Result<(), Self::Err>> + Send;
}

#[derive(Debug)]
pub enum UpdateError<LogErr> {
    // VersionConflict,
    LogErr(LogErr),
    MaxRetriesExceeded,
    DroppedSender
}

pub struct ShardMapReadGuard<S> {
    shard: OwnedRwLockReadGuard<Shard<S>>,
}

impl<S> Deref for ShardMapReadGuard<S> {
    type Target = S;
    fn deref(&self) -> &Self::Target { &self.shard.state }
}

pub struct Shard<S> {
    state: S,
    state_nonce: Arc<std::sync::atomic::AtomicU64>,
    sync_waitlist: Arc<Semaphore>,
    last_sync_finished_at: Option<Instant>
}

impl<S: Default> Default for Shard<S> {
    fn default() -> Self {
        Self{
            state: Default::default(),
            state_nonce: Default::default(),
            sync_waitlist: Arc::new(Semaphore::new(1)),
            last_sync_finished_at: None
        }
    }
}

pub type CommandReturnSender<R> = oneshot::Sender<Result<R, Nonce>>;
pub type CommandReturnReceiver<R> = oneshot::Receiver<Result<R, Nonce>>;

pub struct ShardMap<K, S, C: Command<S>> {
    // RwLock<Arc<RwLock<_>>> is not ideal, but no cause for premature optimization
    // Possible alternatives: dashmap (beware, deadlock minefield!), contrie, flurry, ssc::HashMap
    shards: RwLock<BTreeMap<K, Arc<RwLock<Shard<S>>>>>,

    nonce_subs: Arc<Mutex<BTreeMap<Nonce, CommandReturnSender<C::Ret>>>>,

    nonce_gen: Box<dyn Fn() -> Nonce + Send + Sync>,
}

impl<K, S, C: Command<S>> ShardMap<K, S, C> {
    pub fn new(nonce_gen: impl Fn() -> Nonce + Send + Sync + 'static) -> Self {
        Self{
            shards: Default::default(),
            nonce_subs: Default::default(),
            nonce_gen: Box::new(nonce_gen)
        }
    }
}

impl<K, S, C> ShardMap<K, S, C>
where
    K: Ord + Clone + Send + Sync,
    S: Default + Send + Sync + 'static,
    C: Command<S>
{
    pub async fn all_keys(&self) -> Vec<K> {
        self.shards.read().await.keys().cloned().collect()
    }

    pub async fn get(&self, key: &K) -> Option<ShardMapReadGuard<S>> {
        let ptr = self.shards.read().await.get(key)?.clone();
        let shard = ptr.read_owned().await;
        Some(ShardMapReadGuard{ shard })
    }

    async fn get_ptr_or_default(&self, key: &K) ->  Arc<RwLock<Shard<S>>> {
        if let Some(p) = self.shards.read().await.get(key) {
            return p.clone()
        }
        // NOTE This is the single place where we write to the entries-BTreeMap.
        //      The above `if let` must not be rewritten to a `match`, else there will be a deadlock
        self.shards.write().await.entry(key.clone()).or_default().clone()
    }

    async fn sync_loop<L>(&self, log: &L, key: &K, max_staleness: Option<Duration>) -> Result<Nonce, L::Err>
        where L: DistributedLog<K, C>
    {

        let shard_ptr = self.shards.read().await.get(key).unwrap().clone();

        let shard = shard_ptr.read().await;
        if let Some(max_staleness) = max_staleness {
            let last_sync_finished_at = shard.last_sync_finished_at;

            if last_sync_finished_at > Some(Instant::now().checked_sub(max_staleness).unwrap()) {
                // no need to sync yet, we just synched 50ms ago
                return Ok(Nonce(shard.state_nonce.load(std::sync::atomic::Ordering::SeqCst)))
            }
        }
        let state_nonce_ptr = shard.state_nonce.clone();
        drop(shard);

        struct ReceiveFnImpl<S, R> {
            shard_ptr: Arc<RwLock<Shard<S>>>,
            state_nonce_ptr: Arc<std::sync::atomic::AtomicU64>,
            nonce_subs: Arc<Mutex<BTreeMap<Nonce, CommandReturnSender<R>>>>
        }

        impl<S, C> CommandReceiveFn<C> for ReceiveFnImpl<S, C::Ret>
            where C: Command<S>, S: Send + Sync
        {
            async fn receive(&mut self, recv_msg: ReceiveCommand<C>){
                let state_nonce = Nonce(self.state_nonce_ptr.load(std::sync::atomic::Ordering::SeqCst));

                println!("{:?} {:?}", recv_msg.expected_nonce, state_nonce);
                if recv_msg.expected_nonce == state_nonce {
                    println!("update state and nonce");
                    // update state and nonce
                    let mut shard = self.shard_ptr.write().await;
                    let ret = recv_msg.command.apply(&mut shard.state);
                    drop(shard);
                    // TODO assert state_nonce_ptr has not changed
                    self.state_nonce_ptr.store(recv_msg.new_nonce.0, std::sync::atomic::Ordering::SeqCst);

                    // TODO remove
                    let state_nonce = Nonce(self.state_nonce_ptr.load(std::sync::atomic::Ordering::SeqCst));
                    println!("new nonce {state_nonce:?}");

                    // notify subscriber of write-success with command return
                    if let Some(sub) = self.nonce_subs.lock().await.remove(&recv_msg.new_nonce) {
                        let _ = sub.send(Ok(ret));
                    }
                } else {
                    // notify subscriber of write-conflict with current state nonce
                    if let Some(sub) = self.nonce_subs.lock().await.remove(&recv_msg.new_nonce) {
                        let _ = sub.send(Err(state_nonce));
                    }
                }
            }
        }

        let result = log.receive(key, &mut ReceiveFnImpl{
            shard_ptr: shard_ptr.clone(),
            state_nonce_ptr: state_nonce_ptr.clone(),
            nonce_subs: self.nonce_subs.clone(),
        }).await;

        /*
        let result = log.receive(key, &mut |recv_msg| {
            let shard_ptr = shard_ptr.clone();
            let state_nonce_ptr = state_nonce_ptr.clone();
            async move {
                let state_nonce = Nonce(state_nonce_ptr.load(std::sync::atomic::Ordering::SeqCst));

                println!("{:?} {:?}", recv_msg.expected_nonce, state_nonce);
                if recv_msg.expected_nonce == state_nonce {
                    // update state and nonce
                    let mut shard = shard_ptr.write().await;
                    let ret = recv_msg.command.apply(&mut shard.state);
                    drop(shard);
                    // TODO assert state_nonce_ptr has not changed
                    state_nonce_ptr.store(recv_msg.new_nonce.0, std::sync::atomic::Ordering::SeqCst);

                    // notify subscriber of write-success with command return
                    if let Some(sub) = self.nonce_subs.lock().await.remove(&recv_msg.new_nonce) {
                        let _ = sub.send(Ok(ret));
                    }
                } else {
                    // notify subscriber of write-conflict with current state nonce
                    if let Some(sub) = self.nonce_subs.lock().await.remove(&recv_msg.new_nonce) {
                        let _ = sub.send(Err(state_nonce));
                    }
                }
            }
        }).await;
    */

        // FIXME if the sync_loop() future gets dropped, then shard.state_nonce may not get properly updated
        let mut shard = shard_ptr.write().await;
        shard.last_sync_finished_at = Some(Instant::now());
        drop(shard);

        let last_nonce = Nonce(state_nonce_ptr.load(std::sync::atomic::Ordering::SeqCst));
        result.map(|_| last_nonce)
    }

    // returns last nonce for debugging purposes
    pub async fn sync_shard<L>(&self,
        log: &L,
        key: &K,
        send_command: Option<&C>,
        max_staleness: Option<Duration>,
    ) -> Result<Option<CommandReturnReceiver<C::Ret>>, L::Err>
        where L: DistributedLog<K, C>
    {
        // TODO only default-initialize later on, when we KNOW the entry exists
        let shard_ptr = self.get_ptr_or_default(key).await;

        // if someone else is synching, we join the process waitlist until they are finished
        let shard = shard_ptr.read().await;
        let sync_waitlist = shard.sync_waitlist.clone();
        drop(shard);
        let permit = sync_waitlist.acquire().await.unwrap();
        
        // sync shard if necessary
        let latest_state_nonce = self.sync_loop(log, key, max_staleness).await?;

        // if we got provided a command, we want to send it & sync again
        if let Some(command) = send_command {
            let new_nonce = (self.nonce_gen)();
            let msg = SendCommand{ command, expected_nonce: latest_state_nonce, new_nonce };
            log.send(key, msg).await?;

            let (tx, rx) = oneshot::channel();
            self.nonce_subs.lock().await
                .insert(new_nonce, tx);

            // sync after sending command
            self.sync_loop(log, key, None).await?;
            
            // next process can take over
            drop(permit);

            Ok(Some(rx))
        } else {
            // next process can take over
            drop(permit);

            Ok(None)
        }
    }

    pub async fn update<L>(&self,
        log: &L,
        key: &K,
        cmd: &C,
        max_retries: u32,
        max_staleness: Duration,
    ) -> Result<(C::Ret, u32), UpdateError<L::Err>>
        where L: DistributedLog<K, C>
    {
        let mut retry_count = 0;
        while retry_count <= max_retries {
            let mut rx = self.sync_shard::<L>(log, key, Some(cmd), Some(max_staleness)).await
                .map_err(UpdateError::LogErr)?
                .unwrap(); // sync_shard always returns Some(rx) if Some(cmd) passed

            match rx.try_recv() {
                Ok(Ok(ret)) => return Ok((ret, retry_count)),
                Ok(Err(_conflict_nonce)) => { retry_count += 1 },
                Err(oneshot::error::TryRecvError::Empty) => todo!("never read back command message"),
                Err(oneshot::error::TryRecvError::Closed) => unreachable!("sender must not be dropped"),
            }
        }
        Err(UpdateError::MaxRetriesExceeded)
    }
}

pub struct Session<K, S, C: Command<S>, L> {
    pub shard_map: Arc<ShardMap<K, S, C>>,
    pub log: Arc<L>,
    max_retries: u32,
    max_write_staleness: Duration,
    max_read_staleness: Duration,
}

impl<K, S, C: Command<S>, L> Session<K, S, C, L> {
    pub fn new(
        nonce_gen: impl Fn() -> Nonce + Send + Sync + 'static,
        log: Arc<L>,
        max_retries: u32,
        max_write_staleness: Duration,
        max_read_staleness: Duration,
    ) -> Self {
        let shard_map = Arc::new(ShardMap::new(nonce_gen));
        Self{ shard_map, log, max_retries, max_write_staleness, max_read_staleness }
    }
}

impl<K, S, C, L> Session<K, S, C, L>
where
    K: Ord + Clone + Send + Sync + 'static,
    S: Default + Send + Sync + 'static,
    C: Command<S>,
    L: DistributedLog<K, C>,
    ShardMap<K, S, C>: Send
{
    pub async fn get(&self, key: &K) -> Option<ShardMapReadGuard<S>> {
        self.shard_map.get(key).await
    }

    pub async fn sync_get(&self, key: &K) -> Result<Option<ShardMapReadGuard<S>>, L::Err> {
        let _ = self.shard_map.sync_shard(&*self.log, key, None, Some(self.max_read_staleness)).await?;
        Ok(self.get(key).await)
    }

    pub async fn update(&self, key: &K, cmd: &C) -> Result<(C::Ret, u32), UpdateError<L::Err>> {
        // let (result, retry_count) = match self.shard_map.update(&self.log, key, cmd, self.max_retries).await {
        //     Ok((ret, retry_count)) => {
        //         (Ok(ret), retry_count)
        //     }
        //     Err(err @ UpdateError::MaxRetriesExceeded) => {
        //         (Err(err), self.max_retries)
        //     }
        //     Err(err) => return Err(err)
        // };

        self.shard_map.update(&*self.log, key, cmd, self.max_retries, self.max_write_staleness).await
    }

    pub async fn sync_all(&self) -> Result<(), L::Err> {
        let keys = self.shard_map.all_keys().await;
        // TODO parallelized this
        for key in keys {
            let _ = self.shard_map.sync_shard(&*self.log, &key, None, Some(self.max_read_staleness)).await?;
        }
        Ok(())
    }
}