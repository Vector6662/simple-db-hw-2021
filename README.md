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

> 该计算模型将关系代数中每一种操作抽象为一个 **Operator**，将整个 SQL 构建成一个 Operator 树，查询树**自顶向下**的调用next()接口，数据则自底向上的被拉取处理。

HeapFile下边的Iter类是数据来源，实现也稍微有点繁琐，主要为了达到的目的是，**每次只加载一个page的数据**。当一个页面的tuples被遍历完毕后，再加载下一个页面。

实现其他的Operator，顶级的Operator是SeqScan，而这个方法只是简单的封装了一下DBFile的DbFileIterator。次一级的是Predicate的JoinPredicate，这是两个Comparator，分别对应Filter和Join，这是两个具体的Operator。

exercise 1的实现比较容易出错的是Join#fetchNext()，简单来说要记录一下当前的t1，和child2依次比较。要将child2比较完之后才可以换下一个（child1.next()）。

exercise 2让我真切感受到了写着写着脑袋就脑袋一团浆糊，捋一下思路，其实就是实现要求的三个类，而且实际上只用实现一个类：`IntegerAggregator`，我感觉`StringAggregator`很多此一举，这个类只是不能用SUM、AVG等聚合函数，只能用COUNT。我的实现是，聚合的field不管是INT还是STRING，都是用hashCode转换成int（见aggregate，`value.getField(index).hashCode()`），因为IntField的hashCode就是value本身，StringField的hashCode也是int，这样如果client要将string用SUM、AVG聚合函数也是可以的，不会给报错，但一般不会这么用吧。

Aggregate是让我纠结最久的，总结了一下其实可以很简单：将构造器中child的数据全部“喂”给aggregator，这样后边所有的迭代(open、fetchNext等)都交给aggregator管理，因为此时aggregator获得了child中的所有tuple。

实验进行到现在还没涉及事务，但是在实现insertTuple和deleteTuple却涉及了一致性问题。目前的解决是，在page被修改了后，便立即修改file，即立即apply，这是因为对page的修改并不会持久化，只有调用writePage后才会apply（落盘）。

lab2文档的2.6、2.7值得一看！尤其是2.6，如果我在开始exercise前看了这个demo的话可能思路会清晰很多。

## Lab3

exercise 1：关键是复现lab3文档中的figure 2。其中b_part实现细节很多，“-1”的原因是不包含b.right边界，因为这里设计的一个space 是左闭又开的[b.left, b.right)，b.right真实的值应该是它距离min的space：`(b_left+1)*space`，然后加上`min`。最终b_part指的是占所处space的比值。我们又假设值在一个柱形中是均匀分布的，所以据此估值：`b_part * h_b`。

```java
double b_part = ((b_left + 1) * space + min - v -1) * 1.0 / space;
```

TableStats中estimateScanCost和estimateTableCardinality两个方法分别表示scan整个表的IO消耗、遍历表内所有的tuples的cpu消耗。由于每次读取一个page，因此IO消耗就是table内page的数量。

Selinger Algorithm感觉原理和矩阵连乘的原理非常相似，这里的exercise 4就是要实现一个join优化器（Join Ordering）。但是比较关键的算法computeCostAndCardOfSubplan已经实现了，只需要按照文档中提到的伪代码实现就可以了，大大降低了难度。我觉得稍后可以看一下computeCostAndCardOfSubplan是怎么实现的，这样才能搞明白Selinger Algorithm如何实现的。



## Lab4

BufferPool的bufferedPages本来想采用LinkedHashMap来实现LRU，但出现了一个很常见的问题：ConcurrentModificationException。出现这个问题是因为调用get方法的时候还是会改变`modCount`，因为LinkedHashMap实现了`afterNodeAccess`方法，这个方法里边就会修改modCount，导致：

```java
if (modCount != expectedModCount)
                throw new ConcurrentModificationException();
```

这个报错参考了：[这里](https://blog.csdn.net/weixin_38802600/article/details/108622827)，讲到了导致的原因和解决方案，关键就是在遍历的同时修改了modCount。



## Lab5

`BTreeInternalPage#deleteEntry`是个很重要的方法，需要解读一下。





## Lab 6

我觉得实现的是STEAL+FORCE，但是这一点文档中好像没有明确说明，如果是NO-STEAL的话应该还会有一个undo log，然而这个lab中并没有。

关注提交(commit)流程应该是这个lab的重点，整个commit过程的起点是`Transaction#transactionComplete()`方法。里边分别会调用buffer pool的transactionComplete方法，和LogFile的logCommit方法。

buffer pool的transactionComplete方法用来完成刷盘，以tid为单位刷盘，调用flushPages方法。

对于一个UPDATE类型的log record，关键的代码是（`BufferPool#flushPage(PageId pid)`）：

```java
Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);//tid, before-image, after-image
```

这里我觉得是整个实现设计非常秒的地方，在日志中同时记录了before-image和after-image，只需要一个logFile便可以同时支持回滚和recover。

而STEAL体现在接下来的代码，事务还没有提交，即还没有在LogFile中追加上COMMIT类型的log record前，就把这个buffer pool中的dirty page给落盘了。

```java
//steal
page.markDirty(false, null);
DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
dbFile.writePage(page);
```

LogFile的logCommit方法其实非常简单，只是在log file中追加一条COMMIT类型的记录即可。



这个lab的代码量应该是最少的，但也是打脑壳的。只需要实现logFile中rollback和recover方法。

rollback的实现一个很重要的技巧，从log file的结尾往前遍历更为简单，这是log record的数据格式决定的，在LogFile的最开始的说明中有讲到：

>Each log record **ends with** a long integer file offset representing the position in the log file where the record began.

具体rollback的方式就比较简单了，用UPDATE记录中的before-image来替换对于的page即可。

目前还没有完成通过测试（9/10），但是还早不到原因。





