#ifndef __THREADPOOL_H__
#define __THREADPOOL_H__

#include <pthread.h>
#include <semaphore.h>

//TODO 没有销毁线程池的操作……

#ifdef  __cplusplus
extern "C" {
#endif

typedef unsigned long int task_id;                 //任务id
typedef void *(*taskfunc)(void *);
struct thrdpool;

struct thrdpool* creatpool(int threadnum);        //创建线程池，参数是线程数


//增加一个任务，执行task函数，参数为param
#define NEEDRET     1
task_id addtask(struct thrdpool* pool, taskfunc func, void* param, uint flags);

//等待并取回结果，必须对每个needretval=1的任务执行，不然会导致类似"僵尸进程"的东西
int waittask(struct thrdpool* pool, task_id id, void** result);

int taskinqueu(struct thrdpool* pool);

void add_delay_job(taskfunc func, void* param, unsigned int delaySec);
#ifdef  __cplusplus
}
#endif  /* end of __cplusplus */

#endif
