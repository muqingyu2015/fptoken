package cn.lxdb.plugins.muqingyu.fptoken;

import cn.lxdb.plugins.muqingyu.fptoken.runner.entry.FptokenLineFileRunnerMain;

/**
 * 兼容入口（类名保留），实际实现迁移到 {@link FptokenLineFileRunnerMain}。
 */
public final class FptokenLineFileEntryMain {

    private FptokenLineFileEntryMain() {
    }

    public static void main(String[] args) throws Exception {
        FptokenLineFileRunnerMain.run(args);
    }
}
