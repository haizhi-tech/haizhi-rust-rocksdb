use haizhi_rocksdb::{Cache, LRUCacheOption};

#[test]
fn lru_cache_explicit_and_automatic_sharding() {
    for bits in [-1, 0, 6, 7, 8, 9] {
        let mut option = LRUCacheOption::default();
        option.set_num_shard_bits(bits);
        let mut cache = Cache::new_lru_cache_with_option(32 << 20, &option).unwrap();
        let other = Cache::new_lru_cache_with_option(32 << 20, &option).unwrap();
        drop(option);
        assert_eq!(other.get_usage(), 0);
        assert_eq!(cache.get_usage(), 0);
        assert_eq!(cache.get_pinned_usage(), 0);
        cache.set_capacity(64 << 20);
        let clone = cache.clone();
        drop(cache);
        assert_eq!(clone.get_usage(), 0);
    }
}

#[test]
fn lru_cache_rejects_invalid_shard_bits() {
    for bits in [i32::MIN, -2, 20, i32::MAX] {
        let mut option = LRUCacheOption::default();
        option.set_num_shard_bits(bits);
        let result = Cache::new_lru_cache_with_option(32 << 20, &option);
        assert!(result.is_err());
        assert!(result.err().unwrap().to_string().contains("num_shard_bits"));
    }
}

#[test]
fn lru_cache_original_constructor_still_works() {
    let cache = Cache::new_lru_cache(32 << 20).unwrap();
    assert_eq!(cache.get_usage(), 0);
    let cache = Cache::new_lru_cache_with_option(32 << 20, &LRUCacheOption::default()).unwrap();
    assert_eq!(cache.get_usage(), 0);
}
