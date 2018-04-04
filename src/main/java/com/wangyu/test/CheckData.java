package com.wangyu.test;


/**
 * Created by wangyu21 on 2017/8/8.
 * 用于解决数据版本问题
 */
public interface CheckData<T> {
    void checkData(T obj);
}
