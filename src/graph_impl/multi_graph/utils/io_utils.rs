extern crate time;

pub fn current_time() -> i64 {
    let timespec = time::get_time();
    timespec.sec * 1000 + (timespec.nsec as f64 / 1000.0 / 1000.0) as i64
}

pub fn get_elapsed_time_in_millis(begin_time: i64) -> f32 {
    (current_time() - begin_time / 1000000.0 as i64) as f32
}
