# 统计

## Wordcount

统计出现的个数

## Wordcount_partitioner

通过分区，小于5字符在一个分区   等于5字符在一个分区   大于5字符在一个分区

## covid_partitioner

通过洲的个数进行分区

## covid_merge

新冠统计  统计出各个洲的累计死亡和累计确诊

## covid_sort

通过统计各个州的累计死亡和累计确诊  通过各州的累计死亡降序排序

## combiner_wordcount

Combiner对map端的输出先做一次合并，以减少在map和reduce节点之间的数据传输量

## covid_grouping

只要两个洲的 state 相同,compare就返回0，则就应该在一组

## Web

通过日志分析数据





