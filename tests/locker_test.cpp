#include "../src/locker.h"
#include <iostream>
#include <thread>

#include <unistd.h>

using namespace std;

// 交错窗口按 1/10 等比缩放, 保序即可; 窗口本身无产品定时器语义
#define WINDOW_US 300000

void lock_read(int i, locker* l){
    l->rlock();
    cout<<"rlock: "<<i<<endl;
    usleep(WINDOW_US);
    l->unrlock();
    cout<<"rlock unlocked: "<<i<<endl;
}

void lock_write(int i, locker* l){
    l->wlock();
    cout<<"wrlock: "<<i<<endl;
    usleep(WINDOW_US);
    l->unwlock();
    cout<<"wlock unlocked: "<<i<<endl;
}


void test1(){
    locker l;
    // 同线程递归: 重复 rlock/wlock 必须拒绝
    assert(l.rlock() == 0);
    assert(l.rlock() == EDEADLK);
    // 唯一读者可原地升级为写者
    assert(l.wlock() == 0);
    assert(l.wlock() == EDEADLK);
    l.unrlock();
    l.unwlock();
    cout<<"---------------"<<endl;
    thread lt1(lock_read, 1, &l);
    thread lt2(lock_read, 2, &l);
    thread wt1(lock_write, 1, &l);
    thread wt2(lock_write, 2, &l);
    wt1.join();
    wt2.join();
    lt1.join();
    lt2.join();
}

void lock_upgrade(int i, locker* l){
    l->rlock();
    cout<<"rlock in upgrade: "<<i<<endl;
    usleep(WINDOW_US);
    l->upgrade();
    cout<<"upgrade in upgrade: "<<i<<endl;
    usleep(WINDOW_US);
    l->unwlock();
    cout<<"wlock unlocked in upgrade: "<<i<<endl;
}

void test2(){
    locker l;
    assert(l.rlock() == 0);
    thread t1(lock_read, 1, &l);
    thread t2(lock_write, 1, &l);
    // 与其他读者共存时升级必须等待读者退出后成功
    assert(l.upgrade() == 0);
    usleep(WINDOW_US);
    cout<<"unwlock in test2"<<endl;
    l.unwlock();
    t2.join();
    t1.join();

    cout<<"---------------"<<endl;
    thread u1(lock_upgrade, 1, &l);
    thread u2(lock_upgrade, 2, &l);
    u1.join();
    u2.join();
}

void lock_downgrade(int i, locker* l){
    l->rlock();
    cout<<"rlock in downgrade: "<<i<<endl;
    usleep(WINDOW_US);
    l->upgrade();
    cout<<"upgrade in downgrade: "<<i<<endl;
    usleep(WINDOW_US);
    l->downgrade();
    cout<<"downgrade in downgrade: "<<i<<endl;
    usleep(WINDOW_US);
    l->unrlock();
    cout<<"wlock unlocked in upgrade: "<<i<<endl;
}

void test3(){
    locker l;
    thread d1(lock_downgrade, 1, &l);
    usleep(WINDOW_US * 4 / 3);
    cout<<"rlock in test3: "<<l.rlock()<<endl;
    d1.join();
    l.unrlock();
    cout<<"unrlock in test3"<<endl;
    cout<<"---------------"<<endl;
    thread d2(lock_downgrade, 2, &l);
    usleep(WINDOW_US * 2 / 3);
    cout<<"wlock in test3: "<<l.wlock()<<endl;
    d2.join();
    l.unwlock();
    cout<<"unwlock in test3"<<endl;
}

int main(){
    test1();
    cout<<"================"<<endl;
    test2();
    cout<<"================"<<endl;
    test3();
}
