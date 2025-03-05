use crate::{Command, CommandReceiveFn, DistributedLog, Nonce, ReceiveCommand, SendCommand, Session};
use tokio::sync::Mutex;
use tokio::time::Duration;
use std::collections::{BTreeSet, BTreeMap, VecDeque};
use std::sync::Arc;

pub fn test_nonce_generator(start_at: u64) -> impl Fn() -> Nonce + Send + Sync {
    assert_ne!(start_at, 0);
    let c = std::sync::atomic::AtomicU64::new(start_at);
    move || Nonce(c.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
}

#[derive(Default)]
struct LocalTopic {
    main: VecDeque<serde_json::Value>,
    shuffle: Vec<serde_json::Value>,
    pub message_count: u32,
}

pub type LocalConsumer = Vec<LocalTopic>;

pub struct LocalLog<K> {
    consumers_by_topic: Arc<Mutex<BTreeMap<K, LocalConsumer>>>,
    num_consumers: u32,
    consumer: u32,
    shuffle: bool,
    simulated_delay_ms: u64,
}

impl<K> LocalLog<K> {
    pub fn new(num_consumers: u32, shuffle: bool, simulated_delay_ms: u64) -> Self {
        Self{ consumers_by_topic: Default::default(), num_consumers, consumer: 0, shuffle, simulated_delay_ms }
    }

    pub fn consumer(&self, consumer: u32) -> Self {
        Self{ consumers_by_topic: self.consumers_by_topic.clone(), consumer, ..Self::new(self.num_consumers, self.shuffle, self.simulated_delay_ms) }
    }

    pub async fn flush_shuffle(&self){
        let mut topics = self.consumers_by_topic.lock().await;
        
        for consumer in topics.values_mut() {
            for topic in consumer {
                shuffle(&mut topic.shuffle);
                topic.main.extend(topic.shuffle.drain(..));
            }
        }
    }

    async fn simulate_delay(&self){
        if self.simulated_delay_ms != 0 {
            let jitter_us = rand_u64() % (self.simulated_delay_ms * 1000 / 10);
            tokio::time::sleep(Duration::from_micros(self.simulated_delay_ms * 1000 + jitter_us)).await
        }
    }
}

impl<K, M> DistributedLog<K, M> for LocalLog<K>
where
    K: Ord + Clone + Send + Sync + 'static,
    M: serde::Serialize + for<'a> serde::Deserialize<'a> + Send + Sync + 'static
{
    type Err = std::convert::Infallible;
    
    // fn send(&self, key: &K, msg: SendCommand<M>) -> impl Future<Output = Result<(), Self::Err>> + Send {
    //     todo!()
    // }

    async fn send(&self, key: &K, msg: SendCommand<'_, M>) -> Result<(), Self::Err> {
        self.simulate_delay().await;

        let msg_json = serde_json::to_value(&msg).unwrap();

        let mut consumers_by_topic = self.consumers_by_topic.lock().await;
        let consumers = if let Some(ks) = consumers_by_topic.get_mut(key) {
            ks
        } else {
            consumers_by_topic.entry(key.clone())
                .or_insert_with(|| (0..self.num_consumers).map(|_| LocalTopic::default()).collect())
        };

        println!("send {msg_json}");

        for topic in consumers {
            if self.shuffle {
                topic.shuffle.push(msg_json.clone());
            } else {
                topic.main.push_back(msg_json.clone());
            }
            topic.message_count += 1;
            println!("send_consumer {msg_json} msgs: {}", topic.message_count);
        }

        Ok(())
    }

    async fn receive(&self, key: &K, f: &mut impl CommandReceiveFn<M>) -> Result<(), Self::Err> {
        self.simulate_delay().await;

        loop {
            let mut consumers_by_topic = self.consumers_by_topic.lock().await;
            let Some(consumers) = consumers_by_topic.get_mut(key) else {
                return Ok(())
            };
            let Some(topic) = consumers.get_mut(self.consumer as usize) else {
                return Ok(())
            };
            let Some(msg_json) = topic.main.pop_front() else {
                if !topic.shuffle.is_empty() {
                    continue // wait for flush_shuffle
                } else {
                    return Ok(()) // no data
                }
            };
            println!("receive {msg_json} msgs {}", topic.main.len());

            let recv_msg: ReceiveCommand<M> = serde_json::from_value(msg_json).unwrap();
            f.receive(recv_msg).await;
        }
    }
}

fn rand_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    use std::collections::hash_map::RandomState;
    RandomState::new().build_hasher().finish()
}

fn shuffle<T>(vec: &mut [T]) {
    if vec.len() < 2 { return }

    let n: usize = vec.len();
    for i in 0..(n - 1) {
        // Generate random index j, such that: i <= j < n
        // The remainder (`%`) after division is always less than the divisor.
        let j = (rand_u64() as usize) % (n - i) + i;
        vec.swap(i, j);
    }
}

#[tokio::test(flavor="multi_thread", worker_threads=2)]
async fn simple_sequence(){
    let seq = [1, 2, 3, 4, 5];

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct PushNum(u32);

    impl Command<Vec<u32>> for PushNum {
        type Ret = (u32, usize);
        fn apply(self, value: &mut Vec<u32>) -> Self::Ret {
            value.push(self.0);
            (self.0, value.len())
        }
    }

    let db: Session<(), Vec<u32>, PushNum, LocalLog<()>> = Session::new(
        test_nonce_generator(1),
        LocalLog::new(1, false, 0).into(),
        0,
        Duration::from_millis(0),
        Duration::from_millis(0)
    );
    
    assert!(db.get(&()).await.is_none());

    for n in seq.iter().copied() {
        let ret = db.update(&(), &PushNum(n)).await.unwrap();
        assert_eq!(ret, ((n, n as usize), 0));
    }

    let state = db.get(&()).await;
    assert_eq!(state.as_ref().map(|s| &s[..]), Some(&seq[..]));
}

#[tokio::test(flavor="multi_thread", worker_threads=4)]
async fn concurrent_conflicts(){
    let set = [1, 2, 3, 4, 5].into_iter().collect::<BTreeSet<_>>();

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct InsertNum(u32);

    impl Command<BTreeSet<u32>> for InsertNum {
        type Ret = bool;
        fn apply(self, value: &mut BTreeSet<u32>) -> Self::Ret {
            value.insert(self.0)
        }
    }

    let log = LocalLog::<()>::new(set.len() as u32 + 1, false, 0);
    let db: Session<(), BTreeSet<u32>, InsertNum, LocalLog<()>> = Session::new(
        test_nonce_generator(1),
        log.into(),
        100,
        Duration::from_millis(0),
        Duration::from_millis(0),
    );
    let db = Arc::new(db);

    // Spawn periodic flushing task
    let db_flush = db.clone();
    let flush_task = tokio::task::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(100));
        for _ in 0..10 {
            interval.tick().await;
            db_flush.log.flush_shuffle().await;
        }
    });

    // Spawn update tasks
    let db_clone = db.clone();
    let set_clone = set.clone();
    let update_task = tokio::task::spawn(async move {
        let tasks: Vec<_> = set_clone.into_iter().enumerate().map(|(i, num)| {
            let log = db_clone.log.consumer(i as u32 + 1);
            let db2: Session<(), BTreeSet<u32>, InsertNum, LocalLog<()>> = Session::new(
                test_nonce_generator((i as u64 + 1) * 100_000),
                log.into(),
                100,
                Duration::from_millis(0),
                Duration::from_millis(0),
            );
            
            tokio::task::spawn(async move {
                let ret = db2.update(&(), &InsertNum(num)).await.unwrap();
                assert!(ret.0);
            })
        }).collect();

        // Wait for all update tasks to complete
        for task in tasks {
            task.await.unwrap();
        }
    });

    // Run both tasks concurrently
    let (res1, res2) = tokio::join!(flush_task, update_task);
    res1.unwrap();
    res2.unwrap();

    let shard = db.sync_get(&()).await.unwrap();
    assert_eq!(shard.as_ref().map(|s| &**s), Some(&set));
    // panic!("{:?}", db.log.keyspaces.lock().await[&()].message_count);
}