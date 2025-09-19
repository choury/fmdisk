#include "threadpool.h"
#include <stdlib.h>
#include <unistd.h>
#include <semaphore.h>
#include <assert.h>
#include <sys/resource.h>
#include <unordered_map>
#include <queue>
#include <atomic>

using namespace std;

struct task_t {
    task_id      taskid;
    taskfunc     func;
    void*        param;
    unsigned int flags;
    timespec     until;
};

struct val_t{
    pthread_cond_t   cond;
    int              done;
    int              waitc;
    void*            val;
};

struct thrdpool{
    size_t           num;
    size_t           doing;
    task_id          curid;     //下一任务分配id
    sem_t            wait;      //每个线程等待信号量
    pthread_t*       id;        //线程池各线程
    pthread_mutex_t  lock;      //给tasks和valmap的锁
    queue<task_t *>  tasks;
    unordered_map<task_id, val_t*> valmap;   //结果集合
    std::atomic<bool>       done;
};


void* dotask(thrdpool* pool) {                                //执行任务
    while (1) {
        sem_wait(&pool->wait);
        if (pool->done) {
            return nullptr;
        }
        pthread_mutex_lock(&pool->lock);
        task_t* task = pool->tasks.front();
        pool->tasks.pop();
        pool->doing++;
        pthread_mutex_unlock(&pool->lock);
        void *retval = task->func(task->param);

        pthread_mutex_lock(&pool->lock);
        val_t * val = pool->valmap.at(task->taskid);
        assert(val);
        assert(val->done == 0);
        assert(val->val == nullptr);
        val->done = 1;
        val->val = retval;         //存储结果
        pthread_cond_broadcast(&val->cond); //发信号告诉waittask
        if(val->waitc == 0 && (task->flags & NEEDRET) == 0){
            pool->valmap.erase(task->taskid);
            pthread_cond_destroy(&val->cond);
            free(val);
        }
        free(task);
        pool->doing--;
        pthread_mutex_unlock(&pool->lock);
    }
}

thrdpool* creatpool(size_t threadnum) {
    struct rlimit limit;
    if(getrlimit(RLIMIT_NOFILE, &limit) == 0){
        limit.rlim_cur += 1024 * threadnum;
        setrlimit(RLIMIT_NOFILE, &limit);
    }
    thrdpool* pool = new thrdpool;
    pool->num = threadnum;
    pool->doing = 0;
    pool->curid = 1;
    pool->id = (pthread_t *)malloc(threadnum * sizeof(pthread_t));

    sem_init(&pool->wait, 0 , 0);
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setstacksize(&attr, 20*1024*1024);               //设置20M的栈
    pthread_mutex_init(&pool->lock, NULL);
    for (size_t i = 0; i < threadnum; ++i) {
        pthread_create(pool->id + i, &attr, (taskfunc)dotask, pool);
    }
    pthread_attr_destroy(&attr);
    return pool;
}

void destroypool(struct thrdpool* pool) {
    pool->done.store(true);
    for (size_t i = 0; i < pool->num; ++i) {
        sem_post(&pool->wait);
    }
    for (size_t i = 0; i < pool->num; ++i) {
        pthread_join(pool->id[i], nullptr);
    }
    while (!pool->tasks.empty()) {
        task_t* task = pool->tasks.front();
        pool->tasks.pop();
        val_t *val = pool->valmap.at(task->taskid);
        if(val){
            pthread_cond_destroy(&val->cond);
            free(val);
            pool->valmap.erase(task->taskid);
        }
        free(task);
    }
    sem_destroy(&pool->wait);
    pthread_mutex_destroy(&pool->lock);
    free(pool->id);
    delete pool;
}


task_id addtask(thrdpool* pool, taskfunc func, void *param , unsigned int flags) {
    task_t *task = (task_t *)malloc(sizeof(task_t));   //生成一个任务块
    task->param = param;
    task->func = func;
    task->flags = flags;

    val_t *val = (val_t *)malloc(sizeof(val_t));
    val->done = 0;
    val->waitc = 0;
    val->val = nullptr;
    val->cond= PTHREAD_COND_INITIALIZER;

    pthread_mutex_lock(&pool->lock);
    task->taskid = pool->curid++;
    pool->tasks.push(task);                              //加入任务队列尾部
    pool->valmap[task->taskid] = val;
    sem_post(&pool->wait);                                       //发信号给各线程
    auto taskid = task->taskid;
    pthread_mutex_unlock(&pool->lock);
    return taskid;
}


//多线程同时查询，只保证最先返回的能得到结果，其他有可能返回NULL
int waittask(thrdpool* pool, task_id id, void** result) {
    if(id == 0){
recheck:
        pthread_mutex_lock(&pool->lock);
        if(pool->tasks.empty() && pool->doing == 0) {
            pthread_mutex_unlock(&pool->lock);
            *result = nullptr;
            return 0;
        }
        while(!pool->valmap.empty()) {
            pthread_cond_wait(&pool->valmap.begin()->second->cond, &pool->lock);   //等待任务结束
        }
        pthread_mutex_unlock(&pool->lock);
        goto recheck;
    }
    pthread_mutex_lock(&pool->lock);
    int count = pool->valmap.count(id);
    if (count == 0) {                              //没有该任务或者已经取回结果，返回NULL
        pthread_mutex_unlock(&pool->lock);
        return -ENOENT;
    }

    val_t *val = pool->valmap.at(id);
    val->waitc++;
    while(val->done == 0){
        assert(val->val == nullptr);
        pthread_cond_wait(&val->cond, &pool->lock);   //等待任务结束
    }

    *result = val->val;
    val->waitc -- ;
    if(val->waitc == 0){                             //没有其他线程在等待结果，做清理操作
        pthread_cond_destroy(&val->cond);
        free(val);
        pool->valmap.erase(id);
    }
    pthread_mutex_unlock(&pool->lock);
    return 0;
}

int taskinqueu(struct thrdpool* pool){
    int tasks;
    pthread_mutex_lock(&pool->lock);
    tasks = pool->tasks.size();
    pthread_mutex_unlock(&pool->lock);
    return tasks;
}

#include <list>
#include <thread>
#include <atomic>

sem_t delay_task;
std::atomic<bool> delay_task_stop(false);
pthread_mutex_t delay_task_lock = PTHREAD_MUTEX_INITIALIZER;
std::list<task_t*> delay_tasks;

//调度线程，按照先进先出的队列来调度
static void do_delay_task() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += 10;
    while(!delay_task_stop) {
#if __APPLE__
        sem_wait(&delay_task);
#else
        sem_timedwait(&delay_task, &ts);             //等待addtask的信号
#endif
        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        ts.tv_sec = now.tv_sec + 10;

        pthread_mutex_lock(&delay_task_lock);
        task_t* task = nullptr;
        for(auto i = delay_tasks.begin(); i != delay_tasks.end();){
            auto tv_sec = (*i)->until.tv_sec;
            if(task == nullptr && tv_sec <= now.tv_sec){
                task = *i;
                i = delay_tasks.erase(i);
                continue;
            }
            if(tv_sec < ts.tv_sec){
                ts.tv_sec = tv_sec;
            }
            i++;
        }
        pthread_mutex_unlock(&delay_task_lock);
        if(task){
            task->func(task->param);
            free(task);
        }
    }
}


std::thread* delay_thread;

void start_delay_thread(){
    sem_init(&delay_task, 0 ,0);
    delay_thread = new std::thread(do_delay_task);
}

void stop_delay_thread() {
    delay_task_stop.store(true);
    delay_thread->join();
    delete delay_thread;
    sem_destroy(&delay_task);
    pthread_mutex_destroy(&delay_task_lock);
    for(auto i: delay_tasks){
        free(i);
    }
    delay_tasks.clear();
}


void add_delay_job(taskfunc func, void* param, unsigned int delaySec){
    if (delay_task_stop) {
        return;
    }
    task_t* task = nullptr;
    pthread_mutex_lock(&delay_task_lock);
    for(auto i = delay_tasks.begin(); i != delay_tasks.end(); i++){
        if((*i)->func == func && (*i)->param == param){
            task = *i;
            delay_tasks.erase(i);
            break;
        }
    }
    if(task == nullptr){
        task = (task_t*)malloc(sizeof(task_t));
        task->func = func;
        task->param = param;
    }
    clock_gettime(CLOCK_REALTIME, &task->until);
    task->until.tv_sec += delaySec;
    delay_tasks.emplace_back(task);
    pthread_mutex_unlock(&delay_task_lock);
    sem_post(&delay_task);
}
