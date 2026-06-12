package cn.lxdb.plugins.muqingyu.fptoken.pool;

import cn.lxdb.plugins.muqingyu.fptoken.temp.MillisecondClock;

/**
 * 对象池生命周期判定用的毫秒时钟（测试可替换）。
 */
@FunctionalInterface
public interface FpHashMapPoolClock {

	FpHashMapPoolClock SYSTEM = MillisecondClock.CLOCK::now;

	long nowMs();
}
