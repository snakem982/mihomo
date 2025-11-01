// This file is part of the Mihomo project: https://github.com/vernesong/mihomo
// Copyright (C) 2025 vernesong and contributors
//
// This file is licensed under the GNU General Public License v3.0.
// You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.html

package cachefile

import (
	"sync"

	"github.com/metacubex/bbolt"
	"github.com/metacubex/mihomo/component/smart"
	"github.com/metacubex/mihomo/log"
)

var (
	smartInitOnce sync.Once
	smartStore    *smart.Store
)

func GetSmartStore() *smart.Store {
	cache := Cache()
	if cache == nil || cache.DB == nil {
		log.Fatalln("[Smart] DB Cache file load failed")
	}

	smartInitOnce.Do(func() {
		err := cache.DB.Update(func(tx *bbolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(bucketSmartStats)
			return err
		})
		if err != nil {
			log.Fatalln("[SmartStore] Failed to create bucket: %v", err)
		}
		smartStore = smart.NewStore(cache.DB)
	})

	return smartStore
}
