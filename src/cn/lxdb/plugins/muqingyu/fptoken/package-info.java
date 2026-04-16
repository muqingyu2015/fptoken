/**
 * Author: muqingyu
 *
 * 这是我接触LXDB后独立设计的第一个技术方案。目前行业内二进制检索的主流方案主要有两种：ngram和Data Skipping Index，
 * 其余多为两者的变种。ngram的缺点是数据膨胀率非常高，存储成本大，大规模数据下性能严重下降。Data Skipping Index在
 * 命中词条特别多时效率急剧恶化，无法保证稳定查询性能。因此我需要一个从根源上解决上述问题的新方案。
 *
 * 核心思路：借鉴频繁项集思想，通过识别频繁项来控制膨胀率，同时提升高命中词条的查找效率；在此基础上结合
 * Data Skipping Index思路，优化低频命中词条的检索性能。通过将两者有机融合，最终实现一个兼顾存储效率和查询
 * 性能的二进制检索方案。
 */
package cn.lxdb.plugins.muqingyu.fptoken;
