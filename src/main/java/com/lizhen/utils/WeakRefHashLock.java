package com.lizhen.utils;

import org.springframework.stereotype.Component;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 弱引用锁，给某个kafka的消息（这里用id来标识）上锁协调多个topic消费逻辑
 *
 * 为什么要新写一个绑定kafka消息的弱引用锁呢？因为弱引用可以让jvm在内存不够的时候自动回收这些没有绑定的无用的锁，
 * 不需要关心频繁加锁而可能导致内存溢出的问题
 *
 * 在锁使用完成之后，id=1对应的锁就可以被回收处理，且在锁的使用过程中，其余线程尝试获取id=1对应的锁时，和当前线程所持有的锁对象必须为同一个
 *
 * @author ：li zhen
 * @description:
 * @date ：2022/2/22 13:53
 */
@Component
public class WeakRefHashLock {

    /**
     * 存储 key 对应 锁 的弱引用
     */
    private ConcurrentHashMap<Object, LockWeakReference> lockMap = new ConcurrentHashMap<>();

    /**
     * 存储已过期的 ref
     */
    private ReferenceQueue<ReentrantLock> queue = new ReferenceQueue<>();

    /**
     * 获取 key 对应的 lock
     * @param key
     * @return
     */
    public Lock lock(Object key){

        if (lockMap.size() > 1000){
            clearEmptyRef();
        }

        // 获取 key 对应的 lock 弱引用
        LockWeakReference weakReference = lockMap.get(key);

        // 获取lock
        ReentrantLock lock = (weakReference == null ? null : weakReference.get());

        // 这里使用 while 循环获取，防止在获取过程中lock被gc回收
        while (lock == null){
            // 使用 putIfAbsent，在多线程环境下，针对同一 key ，weakReference 均指向同一弱引用对象
            // 这里使用 可重入锁
            weakReference = lockMap.putIfAbsent(key, new LockWeakReference(key, new ReentrantLock(), queue));

            // 获取弱引用指向的lock，这里如果获取到 lock 对象值，将会使 lock 对象值的弱引用提升为强引用，不会被gc回收
            lock = (weakReference == null ? null : weakReference.get());

            // 在 putIfAbsent 的执行和 weakReference.get() 执行的间隙，可能存在执行gc的过程，会导致 lock 为null，所以使用while循环获取
            if (lock != null){
                return lock;
            }

            // 获取不到 lock，移除map中无用的ref
            clearEmptyRef();

        }
        return lock;
    }

    /**
     * 清除 map 中已被回收的 ref
     */
    void clearEmptyRef(){

        Reference<? extends ReentrantLock> ref;
        while ((ref = queue.poll()) != null){
            LockWeakReference lockWeakReference = (LockWeakReference) ref;
            lockMap.remove(lockWeakReference.key);
        }

    }

    class LockWeakReference extends WeakReference<ReentrantLock> {

        /**
         * 存储 弱引用 对应的 key 值，方便 之后的 remove 操作
         */
        private Object key;

        public LockWeakReference(Object key, ReentrantLock lock, ReferenceQueue<? super ReentrantLock> q) {
            super(lock, q);
            this.key = key;
        }

    }
}
