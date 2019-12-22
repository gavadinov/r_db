use super::storage::Shard;

fn a() {
    let mut s = Shard::new(7);

    let v: Vec<u8> = vec![1, 2];
    let r = s.get(&v);
}
