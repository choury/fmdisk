#include "../src/locker.h"
#include <iostream>
#include <thread>

#include <unistd.h>

using namespace std;

void lock_read(int i, locker* l){
    l->rlock();
    cout<<"rlock: "<<i<<endl;
    sleep(3);
    l->unrlock();
    cout<<"rlock unlocked: "<<i<<endl;
}

void lock_write(int i, locker* l){
    l->wlock();
    cout<<"wrlock: "<<i<<endl;
    sleep(3);
    l->unwlock();
    cout<<"wlock unlocked: "<<i<<endl;
}


void test1(){
    locker l;
    cout<<"rlock once in test1: "<<l.rlock()<<endl;
    cout<<"rlock twice in test1: "<<l.rlock()<<endl;
    cout<<"wlock once in test1: "<<l.wlock()<<endl;
    cout<<"wlock twice in test1: "<<l.wlock()<<endl;
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
    sleep(3);
    l->upgrade();
    cout<<"upgrade in upgrade: "<<i<<endl;
    sleep(3);
    l->unwlock();
    cout<<"wlock unlocked in upgrade: "<<i<<endl;
}

void test2(){
    locker l;
    cout<<"rlock in test2: "<<l.rlock()<<endl;
    thread t1(lock_read, 1, &l);
    thread t2(lock_write, 1, &l);
    cout<<"upgrade in test2: "<<l.upgrade()<<endl;
    sleep(3);
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
    sleep(3);
    l->upgrade();
    cout<<"upgrade in downgrade: "<<i<<endl;
    sleep(3);
    l->downgrade();
    cout<<"downgrade in downgrade: "<<i<<endl;
    sleep(3);
    l->unrlock();
    cout<<"wlock unlocked in upgrade: "<<i<<endl;
}

void test3(){
    locker l;
    thread d1(lock_downgrade, 1, &l);
    sleep(4);
    cout<<"rlock in test3: "<<l.rlock()<<endl;
    d1.join();
    l.unrlock();
    cout<<"unrlock in test3"<<endl;
    cout<<"---------------"<<endl;
    thread d2(lock_downgrade, 2, &l);
    sleep(2);
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
