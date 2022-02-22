package utils

import (
	"bytes"
	"math/rand"
	"sync"
	"time"
	"github.com/hardcore-os/corekv/utils/codec"
)

const (
	defaultMaxLevel = 48
)

type SkipList struct {
	header *Element//虚拟的头结点，entry中的key和value都为空

	rand *rand.Rand

	maxLevel int//跳表的最大高度
	length   int//跳表的当前高度
	lock     sync.RWMutex//用于处理并发，读写锁即可
	size     int64
}

func NewSkipList() *SkipList {
	//初始化跳表
	return &SkipList{
		header: newElement(0, codec.NewEntry(nil, nil), defaultMaxLevel),//初始化头结点元素，entry对象key value为nil，初始高度为默认最高
		rand: rand.New(rand.NewSource(time.Now().Unix())),//暂时不明白干什么用的
		maxLevel: defaultMaxLevel,//当前默认跳表最大高度
		length: 1,//当前跳表高度
		lock: sync.RWMutex{},//创建读写锁用于并发支持
		size: 0,//暂时不明白干什么用的
	}
}

type Element struct {
	levels []*Element//这里存放的是元素指针的切片
	entry  *codec.Entry//元素主体
	score  float64//用于加速查找，将KEY的前8个字节HASH计算后，以score作为插入和查询的依据
}

func newElement(score float64, entry *codec.Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level),//这里存放的是元素指针的切片，不直接存储元素主体（空间大）
		entry:  entry,
		score:  score,
	}
}

func (elem *Element) Entry() *codec.Entry {
	return elem.entry
}

func (list *SkipList) Add(data *codec.Entry) error {
	//添加新元素前，先随机新元素level层数，计算score
	list.lock.Lock()
	defer list.lock.Unlock()//加写锁
	var elem *Element
	level:=list.randLevel()
	score:=list.calcScore(data.Key)
	
	//查找到对应位置后，将新元素插入，需要找到level每一层的前一个节点
	preElem:=list.header//用于缓存记录上一节点
	preElemHeaders:=make([]*Element, defaultMaxLevel) //用于记录每一层前一节点的路径
	max:=list.maxLevel//当前跳表的高度! 这里不能用当前最高高度，否则新节点level高于当前值时，记录路径会有一部分记录不到，导致panic
	for i:=max-1;i>=0;i--{
		preElemHeaders[i]=preElem
		for next:=preElem.levels[i];next!=nil;{//perElem 的 leverls中存放的就是下一个元素
			//加判断条件
			if comp:=list.compare(score, data.Key, next);comp<=0{
				if comp == 0{
					//查找到则更新
					elem = next
					elem.entry = data 
					return nil
				}
				//找到要插入地点
				break
			}
			//未找到，则pre、next向后移动
			preElem = next
			preElemHeaders[i] = preElem
			next = preElem.levels[i]
		}
	}
	
	elem=newElement(score, data, level)//初始化，开始插入
	
	for i:=0;i<level;i++{
		elem.levels[i] = preElemHeaders[i].levels[i]
		preElemHeaders[i].levels[i]=elem
	}
	if level>list.length{
		list.length = level
	}
	return nil
}

func (list *SkipList) Search(key []byte) (e *codec.Entry) {
	//查找方式和插入方式差不多
	max:=list.length
	preElem:=list.header
	score:=list.calcScore(key)
	for i:=max-1;i>=0;i--{
		for next:=preElem.levels[i];next!=nil;{
			if comp:=list.compare(score, key, next);comp<=0{
				if comp ==0{
					return next.entry
				}
				//查找元素小于next，则向下层移动
				break
			}
			//查找元素大于下一元素，则向后移动
			preElem = next
			next = preElem.levels[i]
		}
	}
	return nil
}

func (list *SkipList) Close() error {
	return nil
}

func (list *SkipList) calcScore(key []byte) (score float64) {//scroe的计算可以看一下 redis等方案的实现
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}
	//将KEY的前 8 个字节，每一个字节转化为 uint64，左移56 48 40。。。8位，|操作转化为 float64
	score = float64(hash)
	return
}

func (list *SkipList) compare(score float64, key []byte, next *Element) int {
	//先比较score，如果相等再比对字符串key，小加速
	if score == next.score{
		return bytes.Compare(key, next.entry.Key)
	}
	if score < next.score{
		return -1
	}
	return 1
}

func (list *SkipList) randLevel() int {
	//创建元素前进行level随机初始化
	if list.maxLevel <= 1{
		return 1
	}
	i:=1
	for ;i<list.maxLevel;i++{
		if RandN(1000)%2 == 0{//随机概率 1（1/2） 2(1/4)  3(1/8)
			return i
		}
	}
	return i
}

func (list *SkipList) Size() int64 {
	//测试函数中没有使用
	return 0
}
