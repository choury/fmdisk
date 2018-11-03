#ifndef DEFER_H__
#include <functional>

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

#define defer(...) __defer __(__VA_ARGS__)

#endif
