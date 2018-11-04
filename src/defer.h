#ifndef DEFER_H__
#include <functional>
#include <thread>

class __defer{
    std::function<void()> func;
public:
    template<typename _Callable, typename... _Args>
    explicit __defer(_Callable&& __f, _Args&&... __args){
        func = std::bind(std::forward<_Callable>(__f), std::forward<_Args>(__args)...);
    }
    ~__defer(){
        func();
    }
};


class __delay{
    std::function<void()> func;
    static void __delay_func(int second, __delay *d){
        sleep(second);
        d->func();
        delete d;
    }
    ~__delay(){}
public:
    template<typename _Callable, typename... _Args>
    explicit __delay(int second, _Callable&& __f, _Args&&... __args){
        func = std::bind(std::forward<_Callable>(__f), std::forward<_Args>(__args)...);
        std::thread(__delay_func, second, this).detach();
    }
};

#define defer(...) __defer __(__VA_ARGS__)
#define delay(second, ...) (new __delay(second, __VA_ARGS__))

#endif
