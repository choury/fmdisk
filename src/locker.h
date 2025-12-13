#ifndef LOCKER_H__
#define LOCKER_H__

#include <errno.h>
#include <assert.h>
#include <pthread.h>

#include <set>
#include <memory>

class locker{
    pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
    pthread_t writer = 0;
    std::set<pthread_t> reader;
public:
    virtual ~locker(){
        pthread_cond_destroy(&cond);
        pthread_mutex_destroy(&lock);
    }
    virtual int rlock(){
        pthread_t self = pthread_self();
        pthread_mutex_lock(&lock);
        if(writer == self){
            pthread_mutex_unlock(&lock);
            return EDEADLK;
        }
        while(writer){
            pthread_cond_wait(&cond, &lock);
        }
        if(reader.count(self)){
            pthread_mutex_unlock(&lock);
            return EDEADLK;
        }else{
            reader.insert(self);
            pthread_mutex_unlock(&lock);
            return 0;
        }
    }
    virtual int tryrlock(){
        pthread_t self = pthread_self();
        pthread_mutex_lock(&lock);
        if(writer == self){
            pthread_mutex_unlock(&lock);
            return EDEADLK;
        }
        if(writer){
            pthread_mutex_unlock(&lock);
            return EAGAIN;
        }
        if(reader.count(self)){
            pthread_mutex_unlock(&lock);
            return EDEADLK;
        }else{
            reader.insert(self);
            pthread_mutex_unlock(&lock);
            return 0;
        }
    }
    virtual int wlock(){
        pthread_t self = pthread_self();
        pthread_mutex_lock(&lock);
        if(writer == self){
            pthread_mutex_unlock(&lock);
            return EDEADLK;
        }
        while(true){
            if(!writer && reader.empty()){
                break;
            }
            if(!writer && reader.size() == 1 && reader.count(self)){
                break;
            }
            pthread_cond_wait(&cond, &lock);
        }
        writer = self;
        pthread_mutex_unlock(&lock);
        return 0;
    }
    virtual int trywlock(){
        pthread_t self = pthread_self();
        pthread_mutex_lock(&lock);
        if(writer == self){
            pthread_mutex_unlock(&lock);
            return EDEADLK;
        }
        if(!writer && reader.empty()){
            writer = self;
            pthread_mutex_unlock(&lock);
            return 0;
        }
        if(!writer && reader.size() == 1 && reader.count(self)){
            reader.erase(self);
            writer = self;
            pthread_mutex_unlock(&lock);
            return 0;
        }
        pthread_mutex_unlock(&lock);
        return EAGAIN;
    }
    //upgrade会先释放读锁再获取写锁，因此调用者需要在upgrade后重新检查临界区状态
    virtual int upgrade(){
        pthread_t self = pthread_self();
        pthread_mutex_lock(&lock);
        if(writer == self){
            pthread_mutex_unlock(&lock);
            return EEXIST;
        }
        assert(reader.count(self));
        reader.erase(self);
        while(true){
            if(!writer && reader.empty()){
                break;
            }
            pthread_cond_wait(&cond, &lock);
        }
        writer = self;
        pthread_mutex_unlock(&lock);
        return 0;
    }
    virtual void downgrade(){
        pthread_t self = pthread_self();
        pthread_mutex_lock(&lock);
        assert(writer == self);
        assert(reader.count(self) == 0);
        reader.insert(self);
        writer = 0;
        pthread_cond_signal(&cond);
        pthread_mutex_unlock(&lock);
    }
    virtual void unrlock(){
        pthread_mutex_lock(&lock);
        assert(reader.count(pthread_self()));
        reader.erase(pthread_self());
        pthread_cond_signal(&cond);
        pthread_mutex_unlock(&lock);
    }
    virtual void unwlock(){
        pthread_mutex_lock(&lock);
        assert(writer == pthread_self());
        writer = 0;
        pthread_cond_broadcast(&cond);
        pthread_mutex_unlock(&lock);
    }
};

class locker_cond {
    pthread_mutex_t internal_mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t internal_cond = PTHREAD_COND_INITIALIZER;

public:
    ~locker_cond() {
        pthread_cond_destroy(&internal_cond);
        pthread_mutex_destroy(&internal_mutex);
    }

    // 持有锁时的等待
    // 逻辑：释放读锁 -> 等待 -> 被唤醒 -> 重新获取读锁
    void wait_read(std::shared_ptr<locker> lock) {
        pthread_mutex_lock(&internal_mutex);
        lock->unrlock();

        pthread_cond_wait(&internal_cond, &internal_mutex);
        pthread_mutex_unlock(&internal_mutex);
        lock->rlock();
    }

    void wait_write(std::shared_ptr<locker> lock) {
        pthread_mutex_lock(&internal_mutex);
        lock->unwlock();
        pthread_cond_wait(&internal_cond, &internal_mutex);
        pthread_mutex_unlock(&internal_mutex);
        lock->wlock();
    }

    // 唤醒一个等待者
    void notify_one() {
        pthread_mutex_lock(&internal_mutex);
        pthread_cond_signal(&internal_cond);
        pthread_mutex_unlock(&internal_mutex);
    }

    // 唤醒所有等待者
    void notify_all() {
        pthread_mutex_lock(&internal_mutex);
        pthread_cond_broadcast(&internal_cond);
        pthread_mutex_unlock(&internal_mutex);
    }
};

class auto_locker{
    bool locked = false;
    pthread_mutex_t* l;
public:
    auto_locker(const auto_locker&) = delete;
    auto_locker(pthread_mutex_t* l):l(l){
        int ret = pthread_mutex_lock(l);
        if(ret == EDEADLK){
            return;
        }
        assert(ret == 0);
        locked = true;
    }
    ~auto_locker(){
        if(!locked){
            return;
        }
        pthread_mutex_unlock(l);
    }
};

class auto_rlocker{
    bool locked = false;
    bool upgraded = false;
    std::shared_ptr<locker> l;
public:
    auto_rlocker(const auto_rlocker&) = delete;
    auto_rlocker(std::shared_ptr<locker> ptr):l(ptr){
        int ret = l->rlock();
        if(ret == EDEADLK){
            return;
        }
        assert(ret == 0);
        locked = true;
    }
    template<typename T>
    auto_rlocker(T* raw_l):auto_rlocker(raw_l->shared_from_this()){
    }
    int upgrade(){
        assert(locked);
        if(upgraded){
            return -EEXIST;
        }
        int ret = l->upgrade();
        assert(ret == 0);
        upgraded = true;
        return ret;
    }
    void unlock(){
        if(!locked){
            return;
        }
        if(upgraded){
            l->unwlock();
        }else{
            l->unrlock();
        }
        locked = false;
        upgraded = false;
    }
    ~auto_rlocker(){
        if(!locked){
            return;
        }
        if(upgraded){
            l->unwlock();
        }else{
            l->unrlock();
        }
    }
};

class auto_wlocker{
    bool locked = false;
    std::shared_ptr<locker> l;
public:
    auto_wlocker(const auto_wlocker&) = delete;
    auto_wlocker(std::shared_ptr<locker> ptr):l(ptr){
        int ret = l->wlock();
        if(ret == EDEADLK){
            return;
        }
        assert(ret == 0);
        locked = true;
    }
    template<typename T>
    auto_wlocker(T* raw_l):auto_wlocker(raw_l->shared_from_this()){
    }
    void unlock(){
        if(!locked){
            return;
        }
        l->unwlock();
        locked = false;
    }
    ~auto_wlocker(){
        if(locked){
            l->unwlock();
        }
    }
};

#define auto_lock(x)  auto_locker __l(x)
#define auto_rlock(x) auto_rlocker __r(x)
#define auto_wlock(x) auto_wlocker __w(x)

#endif
