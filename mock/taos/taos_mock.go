package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/lingfliu/ucs_core/dao"
	"github.com/lingfliu/ucs_core/data/rtdb"
	"github.com/lingfliu/ucs_core/model"
	"github.com/lingfliu/ucs_core/model/meta"
	"github.com/lingfliu/ucs_core/model/msg"
	"github.com/lingfliu/ucs_core/ulog"
	_ "github.com/taosdata/driver-go/v3/taosSql"
)

const (
	TAOS_HOST     = "62.234.16.239:6030"
	TAOS_DATABASE = "ucs"
	TAOS_USERNAME = "root"
	TAOS_PASSWORD = "taosdata"
)

func main() {
	ulog.Config(ulog.LOG_LEVEL_DEBUG, "", false)
	log.Println("Creating DpDao instance...")
	//config taos
	dpDao := rtdb.NewTaosCli(TAOS_HOST, TAOS_DATABASE, TAOS_USERNAME, TAOS_PASSWORD)
	log.Println("Opening database connection...")
	// // 打开数据库连接
	go _task_dao_init(dpDao)
	log.Println("Successfully connected to Taos database!")
	defer dpDao.TaosCli.Close() // 确保在退出时关闭连接

	s := make(chan os.Signal, 1)
	signal.Notify(s, os.Interrupt)
	for {
		select {
		case <-s:
			return
		default:
			time.Sleep(1 * time.Second)
		}
	}
}

func _task_dao_query(dao *dao.DpDao) {
	tic := "2024-01-01 00:00:00.000"
	toc := "2024-11-10 00:00:00.000"

	ptList := dao.Query(tic, toc, 1, 1, &meta.DataMeta{
		Dimen:   4,
		ByteLen: 4,
	})
	for _, pt := range ptList {
		//serialize pt
		data := pt.Data
		for i := 0; i < len(data); i += 4 {
			val := binary.BigEndian.Uint32(data[i : i+4])
			ulog.Log().I("main", fmt.Sprintf("Queried value: %d", val))
			//ulog.Log().I("main", string(pt.Data))
		}
	}
}

func _task_insert(dao *dao.DpDao) {
	dmsg := &msg.DMsg{
		DNodeId: 1,
		Offset:  0,
		Ts:      time.Now().UnixNano() / 1000000,
	}

	dmsg.DataSet = make(map[int]*msg.DMsgData)
	dmsg.DataSet[0] = &msg.DMsgData{
		Meta: &meta.DataMeta{
			DataClass: meta.DATA_CLASS_INT,
			Dimen:     4,
			SampleLen: 1,
		},
		Data: make([]byte, 4*4),
	}

	i := 0
	for i < 4 {
		binary.BigEndian.PutUint32(dmsg.DataSet[0].Data[i*4:(i+1)*4], uint32(i))
		i++
	}

	dao.Insert(dmsg)

	sql := "insert into dp_0_0 using dp tags(0,0,0) values(?, 1,2,3,4)"
	dao.TaosCli.Exec(sql, dmsg.Ts)
	ulog.Log().I("main", fmt.Sprintf("Inserted data: %v", dmsg.DataSet[0].Data))
}

func _task_dao_init(dao *dao.DpDao) {
	dao.Open()
	dao.InitTable(&model.DPoint{
		DataMeta: &meta.DataMeta{
			DataClass: meta.DATA_CLASS_INT,
			Dimen:     4,
		},
	})

	// go _task_insert(dao)
	// go _task_dao_query(dao)
	var wg sync.WaitGroup
	wg.Add(2) // 增加 2 个 goroutine

	go func() {
		defer wg.Done()
		_task_insert(dao)
	}()

	go func() {
		defer wg.Done()
		_task_dao_query(dao)
	}()

	wg.Wait() // 等待所有 goroutine 完成
}
