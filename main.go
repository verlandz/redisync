// Pre:
// - have redis in local
// - open 4 tab terminal and use broadcast all (1 terminal = 1 service)
// - ex cmd: TC=0 go run main.go
package main

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/go-redis/redis"
	redsync "github.com/go-redsync/redsync/v4"
	goredis "github.com/go-redsync/redsync/v4/redis/goredis"
)

type TC struct {
	Name          string
	Expiry, Sleep time.Duration
	Tries         int
}

var (
	TC_IDX int

	// "delayFunc" using default 500ms
	// "expiry" will set differently depends on usecase
	// 	- cron: (expiry = cron interval - "a little sec"), the expiry must less than cron interval to avoid time precision
	//		ex: sometimes it can be..
	// 			1. Get lock first than old key just expired -> this will lead error, because the service saw the key lock still there, even it actually have < 1s ttl leftgoredis
	// 			2. Old key expired than get lock -> good case
	// 	- etc: if the interval can't be determined, pick the value base on timeout of usecase execution / as high as possible
	// 	and let the .Unlock() clear the lock. This to avoid case where the old one still running + key already expired,
	// 	then another .Lock() comming in, which can lead > 1 services running.
	TC_MAP = map[int]TC{
		0: {
			Name:   "Normal case where each services waiting lock for each other",
			Expiry: 5, Sleep: 15, Tries: 32,
		},
		1: {
			Name:   "Some services failed to get value due out of try",
			Expiry: 5, Sleep: 15, Tries: 20,
			/*
			   Explanation:
			   tries:20, delay:500ms
			   max_waiting: tries*delay=10s

			   service-1: start at 0 < max_waiting : true
			   service-2: start at 5 < max_waiting : true
			   service-3: start at 10(lebih lama) < max_waiting : false
			   service-4: start at 15 < max_waiting : false

			   so technically, the services was failed because waiting too long
			*/
		},
		2: {
			Name:   "Same like 0, Sleep is faster than expiry",
			Expiry: 15, Sleep: 2, Tries: 32,
		},
		3: {
			Name: `
				Workaround for cron, where;
				- only 1 service allowed to do the work
				- after that service done, the others aren't allowed to do that
			`,
			Expiry: 13, Sleep: 15, Tries: 1,
		},
		// Others:
		// - When service down after lock, what happen to key? >> the key will expired based on expiry value
		// - When redis down after lock, what happen to service? >> the service still continue the process, but unable to unlock
		// - When redis down before lock, what happen to service? >> the service can't do lock
	}
)

func main() {
	// get env testcase
	TC_IDX, _ = strconv.Atoi(os.Getenv("TC"))

	// init redis and redisync
	redisCli := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	redisCliSync := newRedisync("Test", redisCli)

	// run work
	work(redisCli, redisCliSync)
}

func newRedisync(key string, client *redis.Client) *redsync.Mutex {
	pool := goredis.NewPool(client)
	rs := redsync.New(pool)

	mx := rs.NewMutex(key,
		redsync.WithTries(TC_MAP[TC_IDX].Tries),
		redsync.WithExpiry(TC_MAP[TC_IDX].Expiry*time.Second),
		redsync.WithDriftFactor(0), // ignore clock driver factor
	)
	return mx
}

func work(client *redis.Client, mx *redsync.Mutex) {
	start := time.Now()
	log.Println("Status1:", mx)

	err := mx.Lock()
	if err != nil {
		log.Println("ErrLock:", err)
		return
	}

	log.Println("Status2:", mx)
	defer func() {
		log.Println("Status3:", mx)
		ok, err := mx.Unlock()
		log.Println("Unlock:", ok, err)
		log.Println("Status4:", mx)
	}()

	log.Println("Begin after waiting for", time.Since(start))
	log.Println("Sleeping...")
	time.Sleep(TC_MAP[TC_IDX].Sleep * time.Second)
	log.Println("Wake up after", time.Since(start))
}
