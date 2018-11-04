#include "threadpool.h"
#include <stdlib.h>
#include <unistd.h>
#include <semaphore.h>
#include <assert.h>
#include <unordered_map>
#include <queue>

using namespace std;

struct task_t {
    task_id      taskid;
    taskfunc     func;
    void*        param;
    unsigned int flags;
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
    sem_t            wait;      //每个线程等待信号量
    pthread_t*       id;        //线程池各线程
    pthread_mutex_t  lock;      //给tasks和valmap的锁
    queue<task_t *>  tasks;
    unordered_map<task_id, val_t*> valmap;   //结果集合
};


void* dotask(thrdpool* pool) {                                //执行任务
    while (1) {
        sem_wait(&pool->wait);
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

#if 0
//调度线程，按照先进先出的队列来调度
void sched() {
    int i;
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += 1e5;
    ts.tv_nsec = 0;
    while (1) {
        sem_timedwait(&tasksum, &ts);             //等待addtask的信号
        sem_wait(&trdsum);                        //等待一个空闲进程

        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        ts.tv_sec = now.tv_sec + 1e5;

        pthread_mutex_lock(&poollock);
        for(i = 0; i < pool.num; ++i) {
            if (pool.tsk[i] == 0)break;        //找到空闲进程号
        }
        tasknode* task = nullptr;
        for(auto i = pool.tasks.begin(); i != pool.tasks.end();){
            auto tv_sec = (*i)->until.tv_sec;
            if(task == nullptr && tv_sec <= now.tv_sec){
                task = *i;
                i = pool.tasks.erase(i);
                continue;
            }
            if(tv_sec < ts.tv_sec){
                ts.tv_sec = tv_sec;
            }
            i++;
        }
        if(task){
            pool.tsk[i] = task;              //分配任务
        }
        pthread_mutex_unlock(&poollock);
        if(task){
            sem_post(&task->wait);
            sem_post(pool.wait + i);
        }else{
            sem_post(&trdsum);
        }
    }
}
#endif


thrdpool* creatpool(int threadnum) {
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
    for (long i = 0; i < threadnum; ++i) {
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
    sem_post(&pool->wait);                                       //发信号给各线程
    pthread_mutex_unlock(&pool->lock);
    return task->taskid;
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
