#include "threadpool.h"
#include <stdlib.h>
#include <unistd.h>
#include <semaphore.h>
#include <assert.h>
#include <errno.h>
#include <sys/resource.h>
#include <unordered_map>
#include <queue>

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
    int              num;
    int              doing;
    task_id          curid;     //下一任务分配id
#if __APPLE__
    sem_t*           wait_ptr;
#else
    sem_t            wait;      //每个线程等待信号量
    sem_t*           wait_ptr;
#endif
    pthread_t*       id;        //线程池各线程
    pthread_mutex_t  lock;      //给tasks和valmap的锁
    queue<task_t *>  tasks;
    unordered_map<task_id, val_t*> valmap;   //结果集合
};


void* dotask(thrdpool* pool) {                                //执行任务
    while (1) {
        sem_wait(pool->wait_ptr);
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
        if(val->waitc || (task->flags & NEEDRET)){
            val->done = 1;
            val->val = retval;         //存储结果
            pthread_cond_broadcast(&val->cond); //发信号告诉waittask
        }else{
            pool->valmap.erase(task->taskid);
            pthread_cond_destroy(&val->cond);
            free(val);
        }
        free(task);
        pool->doing--;
        pthread_mutex_unlock(&pool->lock);
    }

    return NULL;
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

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setstacksize(&attr, 20*1024*1024);               //设置20M的栈
    pthread_mutex_init(&pool->lock, NULL);
    for (size_t i = 0; i < threadnum; ++i) {
#if __APPLE__
        char sem_name[100];
        sprintf(sem_name, "fmdisk-threadpool-%zu", i);
        pool->wait_ptr = sem_open(sem_name, O_CREAT|O_EXCL, S_IRWXU, 0);
        sem_unlink(sem_name);
#else
        pool->wait_ptr = &pool->wait;
        sem_init(pool->wait_ptr, 0 , 0);
#endif
        pthread_create(pool->id + i, &attr, (taskfunc)dotask, pool);
    }
    pthread_attr_destroy(&attr);
    return pool;
}


task_id addtask(thrdpool* pool, taskfunc func, void *param , uint flags) {
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
    sem_post(pool->wait_ptr);                                       //发信号给各线程
    auto taskid = task->taskid;
    pthread_mutex_unlock(&pool->lock);
    return taskid;
}


//多线程同时查询，只保证最先返回的能得到结果，其他有可能返回NULL
int waittask(thrdpool* pool, task_id id, void** result) {
    if(id == 0){
recheck:
        pthread_mutex_lock(&pool->lock);
        if(!pool->tasks.empty()){
            pthread_mutex_unlock(&pool->lock);
            sleep(5);
            goto recheck;
        }

        if(pool->doing){
            pthread_mutex_unlock(&pool->lock);
            sleep(5);
            goto recheck;
        }
        pthread_mutex_unlock(&pool->lock);
        *result = nullptr;
        return 0;
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

#if __APPLE__
sem_t* delay_task_ptr;
#else
sem_t delay_task;
sem_t* delay_task_ptr = &delay_task;
#endif
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
        sem_wait(delay_task_ptr);
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
    pthread_mutex_lock(&delay_task_lock);
    for(auto i: delay_tasks){
        i->func(i->param);
        free(i);
    }
    delay_tasks.clear();
    pthread_mutex_unlock(&delay_task_lock);
}


std::thread* delay_thread;

void start_delay_thread(){
#if __APPLE__
    delay_task_ptr = sem_open("fmdisk-delaytask", O_CREAT, S_IRUSR | S_IWUSR, 0);
    sem_unlink("fmdisk-delaytask");
#else
    sem_init(delay_task_ptr, 0 ,0);
#endif
    delay_thread = new std::thread(do_delay_task);
}

void stop_delay_thread() {
    delay_task_stop.store(true);
    delay_thread->join();
    delete delay_thread;
#if __APPLE__
    sem_close(delay_task_ptr);
#else
    sem_destroy(&delay_task);
#endif
    pthread_mutex_destroy(&delay_task_lock);
}


void add_delay_job(taskfunc func, void* param, unsigned int delaySec){
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
    sem_post(delay_task_ptr);
}
