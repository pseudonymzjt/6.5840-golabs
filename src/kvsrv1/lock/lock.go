package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck      kvtest.IKVClerk
	name    string
	version rpc.Tversion
	id      string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{
		ck:      ck,
		name:    lockname,
		version: 0,
		id:      kvtest.RandValue(8),
	}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		val, ver, err := lk.ck.Get(lk.name)
		if err == rpc.ErrNoKey {
			// try inserting
			ok := lk.ck.Put(lk.name, lk.id, 0)
			if ok == rpc.OK {
				return
			}
			time.Sleep(time.Millisecond * 10)
			continue
		}
		if val == "" {
			// try modifying
			ok := lk.ck.Put(lk.name, lk.id, ver)
			if ok == rpc.OK {
				return
			}
			time.Sleep(time.Millisecond * 10)
			continue
		}
		if val == lk.id {
			return
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func (lk *Lock) Release() {
	for {
		val, ver, err := lk.ck.Get(lk.name)
		if err == rpc.ErrNoKey || val != lk.id {
			// log.Fatalf("Failure when releasing a lock %v: state not right\n", lk.name)
			return
		}
		ok := lk.ck.Put(lk.name, "", ver)
		if ok == rpc.OK {
			return
		}
		time.Sleep(time.Millisecond * 10)
	}
}
