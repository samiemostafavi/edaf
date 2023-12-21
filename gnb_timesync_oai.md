
# GNB Time Sync 

The clock starts when RU thread starts. This can be confirmed by looking at `executables/nr-ru.c:1174`.

This is a forever while loop, it loops over subframes which are scheduled by incoming samples from HW devices.

The goal is to sync gnb clock with system clock (REAL_TIME). They are sync but always making sure that gnb clock (Frame timings etc) start at a certain 10ms offset for example.


The following was added at line 1184:
```
// PPDAF system clock frame sync
  LOG_I(PHY,"PPDAF delay starting frames.\n");
  struct timespec curr_real_time;
  clock_gettime(CLOCK_REALTIME, &curr_real_time);
  const long interval_ms = 10; // 10 milliseconds
  // Calculate the next timestamp with a remainder of 0 milliseconds when divided by 10 milliseconds
  long remainder_ms = interval_ms - (curr_real_time.tv_nsec / 1000000) % interval_ms;
  timespec_add_ms(&slot_start, remainder_ms);
  struct timespec curr_time;
  clock_gettime(CLOCK_MONOTONIC, &curr_time);
  struct timespec sleep_time;
  if((slot_start.tv_sec > curr_time.tv_sec) || (slot_start.tv_sec == curr_time.tv_sec && slot_start.tv_nsec > curr_time.tv_nsec)){
    sleep_time = timespec_sub(slot_start,curr_time);
    usleep(sleep_time.tv_nsec * 1e-3);
  }
```
also in the begining of the file, a new function was defined:
```
// PPDAF timespec_add_ms function
void timespec_add_ms(struct timespec *ts, long ms) {
    ts->tv_sec += ms / 1000;
    ts->tv_nsec += (ms % 1000) * 1000000;
    
    // Adjust if nanoseconds overflowed to seconds
    if (ts->tv_nsec >= 1000000000) {
        ts->tv_sec += ts->tv_nsec / 1000000000;
        ts->tv_nsec %= 1000000000;
    }
}
```