use crate::ffi;
use libc::{self, c_void};

pub struct FlushJobInfo {
    pub(crate) inner: *mut ffi::rocksdb_flush_job_info_t,
}

impl FlushJobInfo {
    pub fn cf_name(&self) -> Vec<u8> {
        unsafe {
            let mut name_len = 0;
            let name = ffi::rocksdb_flush_job_info_cf_name(self.inner, &mut name_len);
            if name.is_null() {
                vec![]
            } else {
                let mut vec = vec![0; name_len];
                std::ptr::copy_nonoverlapping(name as *mut u8, vec.as_mut_ptr(), name_len);
                ffi::rocksdb_free(name as *mut c_void);
                vec
            }
        }
    }

    pub fn largest_seqno(&self) -> u64 {
        unsafe { ffi::rocksdb_flush_job_info_largest_seqno(self.inner) }
    }

    pub fn smallest_seqno(&self) -> u64 {
        unsafe { ffi::rocksdb_flush_job_info_smallest_seqno(self.inner) }
    }
}

/// EventListener trait contains a set of call-back functions that will
/// be called when specific RocksDB event happens such as flush.  It can
/// be used as a building block for developing custom features such as
/// stats-collector or external compaction algorithm.
///
/// Note that call-back functions should not run for an extended period of
/// time before the function returns, otherwise RocksDB may be blocked.
/// For more information, please see
/// [doc of rocksdb](https://github.com/facebook/rocksdb/blob/master/include/rocksdb/listener.h).
pub trait EventListener: Send + Sync {
    fn on_flush_begin(&self, _: &FlushJobInfo) {}
    fn on_flush_completed(&self, _: &FlushJobInfo) {}
}

pub unsafe extern "C" fn on_flush_completed<L>(
    raw_self: *mut c_void,
    info: *mut ffi::rocksdb_flush_job_info_t,
) where
    L: EventListener,
{
    let self_ = &mut *(raw_self as *mut L);
    let info = FlushJobInfo { inner: info };
    self_.on_flush_completed(&info);
}

pub unsafe extern "C" fn on_flush_begin<L>(
    raw_self: *mut c_void,
    info: *mut ffi::rocksdb_flush_job_info_t,
) where
    L: EventListener,
{
    let self_ = &mut *(raw_self as *mut L);
    let info = FlushJobInfo { inner: info };
    self_.on_flush_begin(&info);
}
