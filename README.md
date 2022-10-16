course portal: http://dsg.csail.mit.edu/6.830/

[Course-Info](./courseinfo.md)



- [x] lab1
- [x] lab2
- [x] lab3
- [x] lab4
- [x] lab5
- [x] lab6（9/10）



## lab1

exercise 1：让我纠结最久的是Tuple、TupleDesc、Filed的意思，实验给出的文档也解释得不是很好理解。参考了[这里](https://zhang-each.github.io/2022/02/17/simpledb1/)的解释，其实就是一个Tuple中包含了元数据（TupleDesc）和Tuple，Tuple中是一个个数据（Filed）。

用“元组”这个词语感觉就有点绕口，以为和python中的有些相似。查了下**资料**这原来还是数据库中的一个术语，表中的每行就是一个元组，通常称为行，而每一列是一个属性，这里用TupleDesc来描述。

TupleDesc中的getSize()只是每一个属性的fieldType之和，不包含fieldName，这我不是很理解，蹲个后续。

exercise 2：CataLog是Table的集合，目前发现是Table元数据的集合，没有包含元组，所以里边自定义的类叫做TableDesc应该准确些。

实现HeapFile#readPage时候有一个小坑：

```java
RandomAccessFile f = new RandomAccessFile(file, "r");
f.seek(startPos);
f.read(pageBytes, 0, pageSize);
```

这里read方法的第二个参数off并不是指f中的位置，而是指pageBytes这个数组中的其实位置，方法文档中有解释，只是我忘记看了。这个坑感觉还是比较普遍的，在我写cs245-as3项目是时候调用`ByteBuffer`也遇到了这个问题。

Operators：lab1的最后一个exercise涉及了一下，算是承上启下。SeqScan的实现比较简单，只是把`HeapFile`的迭代器封装了一下。难点在于`HeapFile`的迭代器的实现，要实现的目标是遍历整个table的所有tuple，通过依次读取所有的page，然后用各个page的迭代器来遍历tuple。读取page有另外的要求，可以称为“懒加载”，即不直接从磁盘中读取page，而是通过buffer pool。具体见代码实现。这个SeqScan非常重要，是所有operator的起点，也是child operator数据的来源。



## lab2

实现一个查询引擎，这里具体实现的是火山模型/迭代模型，可以参考这里：[「分布式技术专题」三种常见的数据库查询引擎执行模型](http://blog.itpub.net/69982626/viewspace-2756672/)。

> 该计算模型将关系代数中每一种操作（算子）抽象为一个 **Operator**，将整个 SQL 构建成一个 Operator 树，查询树**自顶向下**的调用next()接口，数据则自底向上的被拉取处理。

HeapFile下边的Iter类是数据来源，实现也稍微有点繁琐，主要为了达到的目的是，**每次只加载一个page的数据**。当一个页面的tuples被遍历完毕后，再加载下一个页面。

实现其他的Operator，顶级的Operator是SeqScan，而这个方法只是简单的封装了一下DBFile的DbFileIterator。次一级的是Predicate的JoinPredicate，这是两个Comparator，分别对应Filter和Join，这是两个具体的Operator。

exercise 1的实现比较容易出错的是Join#fetchNext()，简单来说要记录一下当前的t1，和child2依次比较。要将child2比较完之后才可以换下一个（child1.next()）。

exercise 2让我真切感受到了写着写着脑袋就脑袋一团浆糊，捋一下思路，其实就是实现要求的三个类，而且实际上只用实现一个类：`IntegerAggregator`，我感觉`StringAggregator`很多此一举，这个类只是不能用SUM、AVG等聚合函数，只能用COUNT。我的实现是，聚合的field不管是INT还是STRING，都是用hashCode转换成int（见aggregate，`value.getField(index).hashCode()`），因为IntField的hashCode就是value本身，StringField的hashCode也是int，这样如果client要将string用SUM、AVG聚合函数也是可以的，不会给报错，但一般不会这么用吧。

Aggregate是让我纠结最久的，总结了一下其实可以很简单：将构造器中child的数据全部“喂”给aggregator，这样后边所有的迭代(open、fetchNext等)都交给aggregator管理，因为此时aggregator获得了child中的所有tuple。

实验进行到现在还没涉及事务，但是在实现insertTuple和deleteTuple却涉及了一致性问题。目前的解决是，在page被修改了后，便立即修改file，即立即apply，这是因为对page的修改并不会持久化，只有调用writePage后才会apply（落盘）。

lab2文档的2.6、2.7值得一看！尤其是2.6，如果我在开始exercise前看了这个demo的话可能思路会清晰很多。

![img](README.assets/v2-f29b16e0798ee2607e1cae1bb0aa50ca_1440w.jpg)

来源：https://zhuanlan.zhihu.com/p/219516250

## Lab3

这个lab我觉得一大难点是理解一些单词，比如cardinality、selectivity等。

本lab主要实现的是CBO（基于成本的SQL优化器），分为两个子问题：selectivity和cardinality。[SQL 查询优化器浅析](https://juejin.cn/post/7122754431371706404#heading-9)中的解释比较好：

> **选择率（selectivity）** ：对于某一个过滤条件，查询会从表中返回**多大比例**的数据
> **基数（cardinality）** ：基本含义是表的 unique 行数，在查询计划中常指算子需要处理的行数

#### filter selectivity problem

selectivity单词的意思是选择性，但放在这里直译就有点不明所以，我觉得这里应该理解为对**选择谓词(selection predicate)的估值**，“选择谓词”指的是>=、<、=等，比如`select * from table1 where id>20`，其中的 `>` 就是选择谓词，也即Filter算子。

对选择谓词的估值用到的是直方图(histogram)，见本实验文档中的figure 2，关键是直方图中每一个bucket中的值是均匀分布的。代码实现是在`IntHistoGram.estimateSelectivity(...)`。实现的方式还是有点特别，只实现了大于谓词，基于这个谓词就可以实现小于、小于等于、等于等谓词，原理是这样的：已知f>const，可以推出f>=const-1；而f’<=const可以表达成1-f’>1-const，等等。

另一个需要实现的类是`TableStats`，记录的是一个table的所有数据，包括每种类型字段的直方图分布（当然只有int和string两种类型），可以用这些数据来估算不同谓词的selectivity，结果是一个**比值**，分母是ntups（整个table的tuple总数）。

#### join cardinality problem

cardinality指的是“number of tuples produced by”，这个意思就很宽泛了，出现了table cardinality和**join cardinality**。前一个的意思比较好理解，指一张表中的tuple总数。

join cardinality是重点，指的是两张表t1和t2进行join时产生的tuple总数。但是有可能会出现下边这样的情况（来自[牛课sql练习](https://www.nowcoder.com/practice/4dda66e385c443d8a11570a70807d250?tpId=298&tags=&title=&difficulty=0&judgeStatus=0&rp=0&sourceUrl=%2Fexam%2Foj%3Fpage%3D1%26tab%3DSQL%E7%AF%87%26topicId%3D298)）：

```sql
select 
    cust_name,
    Orders.order_num,
    SUM(quantity*item_price) as OrderTotal
from Customers
join Orders on Customers.cust_id=Orders.cust_id #join 1
join OrderItems on OrderItems.order_num=Orders.order_num #join 2
group by cust_name,order_num
order by cust_name,order_num
```

也就是说，有超过两张表进行join，而且`t1 join t2`和`t2 join t1`的cardinality都是不同的，这非常类似典型的DP问题：矩阵连乘；

`JoinOptimizer#estimateTableJoinCardinality()`就是为了解决上边的问题，估算出两个表join时产生的tuples总数，也就是cardinality。具体估算规则见lab3文档中的2.2.4。

```java
// some code goes here
// implement 2.2.4
if (!joinOp.equals(Predicate.Op.EQUALS)) //cond 3: range scan
    return card1 * card2 *3/10;
if (t1pkey && t2pkey) //cond 1
    return Math.min(card1, card2);
if (!t1pkey && !t2pkey) //cond 2: no pkey
    return Math.max(card1, card2);
return t1pkey ? card2 : card1; //cond1
```



另外还得估算join的成本，即`estimateJoinCost()`。lab3文档中直接给出了公式。

```
joincost(t1 join t2) = scancost(t1) + ntups(t1) x scancost(t2) //IO cost
                       + ntups(t1) x ntups(t2)  //CPU cost
```

接下来就是大boss了，实现**Selinger optimizer**，进行根据上边的两个估算值（join cardinality和join cost），选择一个最优的join顺序。逐行分析一下伪代码：

```java
1. j = set of join nodes //j是总长度
2. for (i in 1...|j|): //join node的长度从1开始到j，计算出每种join node长度下的最优值
3.     for s in {all length i subsets of j}
		//s是所有length都为i的集合，遍历所有的s，得到cost最小的那个plan，作为该length下的bestPlan
4.       bestPlan = {}
5.       for s' in {all length d-1 subsets of s} 
    		//s'指s中一个个的join node，比如要join {t1,t2}和{t3,t4}
6.            subplan = optjoin(s') //计算出这个join node的cost
7.            plan = best way to join (s-s') to subplan
8.            if (cost(plan) < cost(bestPlan)) //记录该长度i下的best plan
9.               bestPlan = plan
10.      optjoin(s) = bestPlan
11. return optjoin(j)
```

`optjoin(s) = bestPlan`对应我代码中的实现是：

```java
planCache.addPlan(set, bestPlan.cost, bestPlan.card, bestPlan.plan);
```

目的是记录当前长度length下的最佳的plan，或者说集合s的最佳plan。这里再详细说一下什么叫“all length i subsets of j”：

比如一共有5个table需要join，那么上边的j就是5，当i为3时，那么可能出现的s集合可能有这些情况：{123}，{213}，{312}，{512}，{135}。。。很多种排列组合方式，每一种组合方式都能计算出一个subplan，通过对比subplan中**join cost**值，选出最小值作为bestplan。

exercise 1：关键是复现lab3文档中的figure 2。其中b_part实现细节很多，“-1”的原因是不包含b.right边界，因为这里设计的一个space 是左闭又开的[b.left, b.right)，b.right真实的值应该是它距离min的space：`(b_left+1)*space`，然后加上`min`。最终b_part指的是占所处space的比值。我们又假设值在一个柱形中是均匀分布的，所以据此估值：`b_part * h_b`。

```java
double b_part = ((b_left + 1) * space + min - v -1) * 1.0 / space;
```

TableStats中estimateScanCost和estimateTableCardinality两个方法分别表示scan整个表的IO消耗、遍历表内所有的tuples的cpu消耗。由于每次读取一个page，因此IO消耗就是table内page的数量。



## Lab4

#### `ConcurrentModificationException`原因：

BufferPool的bufferedPages本来想采用`LinkedHashMap`来实现LRU，但有两点需要注意：

1️⃣在实现`BufferPool#evictPage()`时，要避免将dirty page也给evict了；
2️⃣因为用的是`LinkedHashMap`，**它实现了`afterNodeAccess()`**，其实现了LRU算法， 即调用`get()`时，**会改变内部结构**，这是因为实现了LRU，会将访问的元素放在`head->next`，这会触发`modCount++`，导致：

```java
if (modCount != expectedModCount)
                throw new ConcurrentModificationException();
```

这个报错参考了：[《什么是ConcurrentModificationException？在哪些场景下会报该异常？》](https://blog.csdn.net/weixin_38802600/article/details/108622827)，讲到了导致的原因和解决方案。

#### 实现rwLock

这是本lab的关键，我本来是想直接使用`java.util.concurrent`中的`ReadWriteLock`的，但是发现里边有很多的问题，比如下边三个，都是非常棘手的问题：

1. 无法将线程和事务进行绑定，一个事务不一定只以一个线程出现，有可能两个；
2. ❗`ReadWriteLock`不支持**锁升级**，要将读锁变成写锁，需要先释放读锁，然后再加写锁，这个过程的原子性很难保证；
3. `ReadWriteLock`可重入，这一点带来了很大的麻烦，因为虽然是可重入，但是`unlock`的时候，重入了多少次就需要`unlock`几次，重入次数统计起来有点麻烦。

基于上边的一些困难，发现还不如自己实现一个简陋的读写锁，本lab的文档上边也有提示（2.4）。

代码的实现是`transaction/LockManager.java`，关键的数据结构是`PageContext`，记录一个页面的加锁状态：

```java
static class PageContext {
    PageId pageId;
    Set<TransactionId> readTrxs; //shared
    TransactionId writeTrx; // exclusive

    public PageContext(PageId pageId) {
        this.pageId = pageId;
        readTrxs = new HashSet<>();
        writeTrx = null;
    }
}
```

#### 并发控制与死锁检测

当然采用的是两阶段锁，这在6.824课程中也有提及：

1. acquire lock before using lock；
2. hold until done.

这里的实现非常简单：从磁盘中读取page的时候，就立即加锁，也就是在`BufferPool#getPage()`中进行加锁。

死锁的解决方案也比较简单，没有实现高端的死锁检测算法啥的。在调用`LockManager#lock()`时，会传入一个超时时间，如果超时便抛出`TransactionAbortedException`，代表发生了死锁。



## Lab5

`BTreeInternalPage#deleteEntry`是个很重要的方法，需要解读一下。





## Lab6

实现的是NO STEAL+FORCE。

关注提交(commit)流程应该是这个lab的重点，整个commit过程的起点是`Transaction#transactionComplete()`方法。里边分别会调用buffer pool的transactionComplete方法，和LogFile的logCommit方法。

buffer pool的transactionComplete方法用来完成刷盘，以tid为单位刷盘，调用flushPages方法。

对于一个UPDATE类型的log record，关键的代码是（`BufferPool#flushPage(PageId pid)`）：

```java
Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);//tid, before-image, after-image
```

这里我觉得是整个实现设计非常秒的地方，在日志中同时记录了before-image和after-image，只需要一个logFile便可以同时支持rollback和recover。

实际上这应该也是一种通用的解决方案，在6.824课程的“[10.2故障可恢复事务](https://mit-public-courses-cn-translatio.gitbook.io/mit6-824/lecture-10-cloud-replicated-db-aurora/10.2-gu-zhang-ke-hui-fu-shi-wu-crash-recoverable-transaction)”课程中解释了为什么WAL要带上旧值：

> 学生提问：为什么在WAL的log中，需要带上旧的数据值？
>
> Robert教授：在这个简单的数据库中，在WAL中只记录新的数据就可以了。如果出现故障，只需要重新应用所有新的数据即可。但是<u>大部分真实的数据库同时也会在WAL中存储旧的数值</u>，这样对于一个非常长的事务，只要WAL保持更新，**在事务结束之前**，数据库可以提前将更新了的page写入硬盘（STEAL），比如说将Y写入新的数据740。之后如果在事务提交之前故障了，恢复的软件可以发现，事务并没有完成（没有commit记录），所以需要撤回之前的操作，这时，这些旧的数据，例如Y的750，需要被用来撤回之前写入到data page中的操作。对于Aurora来说，实际上也使用了undo/redo日志，用来撤回未完成事务的操作。

```java
//no steal
page.markDirty(false, null);
DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
dbFile.writePage(page);//在事务提交之后才落盘
```

LogFile的logCommit方法实现非常简单，只是在log file中追加一条COMMIT类型的记录即可。



这个lab的代码量应该是最少的，但也是打脑壳的。只需要实现logFile中rollback和recover方法。

rollback的实现一个很重要的技巧，从log file的结尾往前遍历更为简单，这是log record的数据格式决定的，在LogFile的最开始的说明中有讲到：

>Each log record **ends with** a long integer file offset representing the position in the log file where the record began.

具体rollback的方式就比较简单了，用UPDATE记录中的before-image来替换对应的page即可。

目前还没有完全通过测试（9/10），但是还早不到原因。



# SQL优化器

水非常深的数据库方向的分支，lab3只是浅尝则止，我甚至觉得根本没入门。[pingcap/awesome-database-learning](/pingcap/awesome-database-learning)。

《[SQL 查询优化原理与 Volcano Optimizer 介绍](https://zhuanlan.zhihu.com/p/48735419)》是一个纲领性的介绍。

## 谓词下推

算子：下图的每一个黄色节点就是一个算子，叶子节点总是scan。也可以称为谓词；

谓词下推（predicate pushdown）：下边两幅图描述的过程。我觉得大部分的优化应该是将除join以外的谓词下推到join子节点，然后再进行join，这样在join之前就筛掉很过的tuple。



<img src="README.assets/v2-5865e1c9792f2996a606d4d1dce48eaf_1440w.jpg" alt="img" style="zoom: 33%;" /><img src="README.assets/v2-83d73920215665de9c48984c6d79e93d_1440w.jpg" alt="img" style="zoom: 35%;" />

## Join Optimizer和三种多表连接方式

以上的其实就一个重点：谓词下推，实际上SQL优化的难点是Join Optimizer，这应该是一个NP难问题。比如最基本的Selinger optimizer，通过**枚举**所有的join排列的成本，得到best plan。这里的问题是：

> 要不重复地遍历所有不同的关系代数表示本身就是一项相对棘手的算法问题， 即使实现了这样枚举的功能，其巨大的搜索空间也消耗很多计算力——查询优化本身是为了提高查询性能， 如果**优化算法本身的性能堪忧，则执行这一步骤的意义就消失了**。





### Hash Join、Merge Join、Nested Loop

在经过Join Optimizer后，如何进行实际的join？

三种多表连接方式。参考：[多表连接的三种方式详解 hash join、merge join、 nested loop](https://www.cnblogs.com/xqzt/p/4469673.html)

本lab我实现的应该是nested loop，基于CBO。

> Nested loops 工作方式是循环从一张表中读取数据(**驱动表** outer table)，然后访问另一张表（**被查找表** inner table,通常有索引）。驱动表中的每一行与inner表中的相应记录JOIN。类似一个嵌套的循环。



这里只说一下merge join，将关联表的关联列各自做排序，然后从各自的排序表中抽取数据，到另一个排序表中做匹配。但是排序过程耗时是比较长的，如果单纯为了join而**临时**排序，效果远不如hash join好。所以，用这种join方式，一般基准列本身就是排序的，比如主键，或者加上了索引的列。

