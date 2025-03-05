use std::collections::BTreeMap;

pub trait MapUtils<K: Clone, V: Clone> {
    fn insert_if_new(&mut self, k: K, v: V) -> bool;
    fn insert_cloned_if_new(&mut self, k: &K, v: &V) -> bool;
    fn get_or_default(&mut self, k: &K) -> &mut V
        where V: Default;
}

impl<K: Ord + Clone, V: Clone> MapUtils<K, V> for BTreeMap<K, V> {
    fn insert_if_new(&mut self, k: K, v: V) -> bool {
        if self.contains_key(&k) { return false }
        self.insert(k, v);
        true
    }

    fn insert_cloned_if_new(&mut self, k: &K, v: &V) -> bool {
        if self.contains_key(k) { return false }
        self.insert(k.clone(), v.clone());
        true
    }

    fn get_or_default(&mut self, k: &K) -> &mut V
        where V: Default
    {
        // SAFETY: screw you rust, this is safe
        match unsafe { &mut*(self as *mut Self) }.get_mut(k) {
            Some(v) => v,
            None => self.entry(k.clone()).or_default()
        } 
    }
}