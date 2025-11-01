// This file is part of the Mihomo project: https://github.com/vernesong/mihomo
// Copyright (C) 2025 vernesong and contributors
//
// This file is licensed under the GNU General Public License v3.0.
// You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.html

package smart

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/metacubex/mihomo/common/lru"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel"
)

func InitCache() {
	globalCacheParams.mutex.Lock()
	defer globalCacheParams.mutex.Unlock()

	if dataCache != nil {
		return
	}

	globalCacheParams.BatchSaveThreshold = MinBatchThreshLimit
	globalCacheParams.MaxDomains = MinDomainsLimit
	globalCacheParams.PrefetchLimit = MinPrefetchDomainsLimit
	globalCacheParams.CacheMaxSize = MinDomainsLimit + MinPrefetchDomainsLimit
	globalCacheParams.MemoryLimit = getSystemMemoryLimit()

	dataCache = lru.New[string, interface{}](
		lru.WithSize[string, interface{}](globalCacheParams.CacheMaxSize),
		lru.WithAge[string, interface{}](CacheMaxAge),
	)

	domainCache = lru.New[string, string](
		lru.WithSize[string, string](globalCacheParams.MaxDomains),
		lru.WithAge[string, string](CacheMaxAge),
	)

	prefixCountCache = lru.New[string, int](
		lru.WithSize[string, int](2000),
		lru.WithAge[string, int](300),
	)

	nodeStatesCache = lru.New[string, map[string][]byte](
		lru.WithSize[string, map[string][]byte](2000),
		lru.WithAge[string, map[string][]byte](60),
	)

	// lazy refresh
	unwrapCache = lru.New[string, []C.Proxy](
		lru.WithSize[string, []C.Proxy](500),
		lru.WithAge[string, []C.Proxy](300),
	)

	recordCache = lru.New[string, *AtomicStatsRecord](
		lru.WithSize[string, *AtomicStatsRecord](globalCacheParams.MaxDomains/2),
		lru.WithAge[string, *AtomicStatsRecord](CacheMaxAge),
	)
}

// 从全局缓存获取值
func GetCacheValue(cacheKey string) (interface{}, bool) {
	return dataCache.Get(cacheKey)
}

// 设置全局缓存值
func SetCacheValue(cacheKey string, value interface{}) {
	dataCache.Set(cacheKey, value)
}

// 删除全局缓存值
func DeleteCacheValue(cacheKey string) {
	dataCache.Delete(cacheKey)
}

// 按前缀获取缓存值
func GetCacheValuesByPrefix(prefix string) map[string]interface{} {
	return dataCache.FilterByKeyPrefix(prefix)
}

// 按前缀移除缓存值
func RemoveCacheValuesByPrefix(prefix string) {
	dataCache.RemoveByKeyPrefix(prefix)
}

// 存储预取结果
func (s *Store) StorePrefetchResult(group, config string, target string, asnNumber string, isUDP bool, proxyNames []string, weights []float64) {
	if target == "" || len(proxyNames) == 0 {
		return
	}

	targetCacheKey := FormatCacheKey(KeyTypePrefetch, config, group, target)

	var pm PrefetchMap
	if value, found := GetCacheValue(targetCacheKey); found {
		if bv, ok := value.([]byte); ok {
			json.Unmarshal(bv, &pm)
		}
	}
	nodeWeight := NodeWithWeight{Nodes: proxyNames, Weights: weights}
	if isUDP {
		pm.UDP = nodeWeight
	} else {
		pm.TCP = nodeWeight
	}
	data, err := json.Marshal(pm)
	if err != nil {
		return
	}
	SetCacheValue(targetCacheKey, data)

	if asnNumber != "" && !cdnASNs[asnNumber] {
		asnCacheKey := FormatCacheKey(KeyTypePrefetch, config, group, asnNumber)
		refData, err := json.Marshal(PrefetchMap{Ref: targetCacheKey})
		if err != nil {
			return
		}
		SetCacheValue(asnCacheKey, refData)
	}

	appendToGlobalQueue(StoreOperation{
		Type:   OpSavePrefetch,
		Group:  group,
		Config: config,
		Target: target,
		Data:   data,
	})

	needFlush := len(getGlobalQueueSnapshot()) >= GetBatchSaveThreshold()
	if needFlush {
		go s.FlushQueue(true)
	}
}

// 获取预取结果
func (s *Store) GetPrefetchResult(group, config string, target string, asnNumber string, isUDP bool) ([]string, []float64) {
	if target == "" {
		return nil, nil
	}

	findResult := func(pm PrefetchMap) ([]string, []float64) {
		var res NodeWithWeight
		if isUDP {
			res = pm.UDP
		} else {
			res = pm.TCP
		}
		if len(res.Nodes) > 0 && len(res.Weights) == len(res.Nodes) {
			return res.Nodes, res.Weights
		}
		return nil, nil
	}

	if asnNumber != "" && !cdnASNs[asnNumber] {
		asnCacheKey := FormatCacheKey(KeyTypePrefetch, config, group, asnNumber)
		if value, found := GetCacheValue(asnCacheKey); found {
			if bv, ok := value.([]byte); ok {
				var pm PrefetchMap
				if json.Unmarshal(bv, &pm) == nil && pm.Ref != "" {
					if refValue, refFound := GetCacheValue(pm.Ref); refFound {
						if refBv, refOk := refValue.([]byte); refOk {
							var refPm PrefetchMap
							if json.Unmarshal(refBv, &refPm) == nil {
								if nodes, weights := findResult(refPm); nodes != nil {
									return nodes, weights
								}
							}
						}
					}
				}
			}
		}
	}

	if asnNumber == "" || cdnASNs[asnNumber] {
		targetCacheKey := FormatCacheKey(KeyTypePrefetch, config, group, target)
		if value, found := GetCacheValue(targetCacheKey); found {
			if bv, ok := value.([]byte); ok {
				var pm PrefetchMap
				if json.Unmarshal(bv, &pm) == nil {
					if nodes, weights := findResult(pm); nodes != nil {
						return nodes, weights
					}
				}
			}
		}
	}

	ops := getGlobalQueueSnapshot()
	for _, op := range ops {
		if op.Type == OpSavePrefetch && op.Group == group && op.Config == config && op.Target == target {
			var pm PrefetchMap
			if err := json.Unmarshal(op.Data, &pm); err == nil {
				if nodes, weights := findResult(pm); nodes != nil {
					return nodes, weights
				}
			}
		}
	}

	pathPrefix := FormatDBKey("smart", KeyTypePrefetch, config, group, target)
	rawResult, err := s.GetSubBytesByPath(pathPrefix)
	if err != nil {
		return nil, nil
	}

	for _, data := range rawResult {
		var pm PrefetchMap
		if err := json.Unmarshal(data, &pm); err == nil {
			if nodes, weights := findResult(pm); nodes != nil {
				return nodes, weights
			}
		}
	}

	return nil, nil
}

// 预加载所有预计算结果
func (s *Store) LoadAllPrefetchResults(group, config string, limit int) int {
	var (
		loadCount     int
		parseFailures int
	)

	if group == "" || config == "" {
		return 0
	}

	prefetchPrefix := FormatDBKey("smart", KeyTypePrefetch, config, group)
	results, err := s.DBViewPrefixScan(prefetchPrefix, limit)
	if err != nil {
		log.Warnln("[SmartStore] Failed to load prefetch results: %v", err)
		return 0
	}

	for path, v := range results {
		parts := strings.Split(path, "/")
		if len(parts) < 5 {
			continue
		}

		target := parts[4]
		if target == "" {
			continue
		}

		var pm PrefetchMap
		if err := json.Unmarshal(v, &pm); err != nil {
			parseFailures++
			continue
		}

		cacheKey := FormatCacheKey(KeyTypePrefetch, config, group, target)
		SetCacheValue(cacheKey, v)

		loadCount++

		if loadCount >= limit {
			break
		}
	}

	if err != nil {
		log.Warnln("[SmartStore] Failed to load prefetch results: %v", err)
	}

	return loadCount
}

func (s *Store) StoreUnwrapResult(group, config string, target string, asnNumber string, isUDP bool, proxies []C.Proxy) {
	if target == "" || len(proxies) == 0 {
		return
	}

	resultType := WeightTypeTCP
	if isUDP {
		resultType = WeightTypeUDP
	}

	key := fmt.Sprintf("%s:%s:%s:%s", config, group, resultType, target)

	if asnNumber != "" {
		key = fmt.Sprintf("%s:%s:%s:%s", config, group, resultType, asnNumber)
	}

	unwrapCache.Set(key, proxies)
}

func (s *Store) GetUnwrapResult(group, config, target, asnNumber string, isUDP bool) []C.Proxy {
	if target == "" {
		return nil
	}

	resultType := WeightTypeTCP
	if isUDP {
		resultType = WeightTypeUDP
	}

	key := fmt.Sprintf("%s:%s:%s:%s", config, group, resultType, target)

	if asnNumber != "" {
		key = fmt.Sprintf("%s:%s:%s:%s", config, group, resultType, asnNumber)
	}

	if value, ok := unwrapCache.Get(key); ok {
		return value
	}

	return nil
}

func (s *Store) DeleteUnwrapResult(group, config string, target string, asnNumber string, isUDP bool) {
	if target == "" {
		return
	}

	resultType := WeightTypeTCP
	if isUDP {
		resultType = WeightTypeUDP
	}

	key := fmt.Sprintf("%s:%s:%s:%s", config, group, resultType, target)

	if asnNumber != "" {
		key = fmt.Sprintf("%s:%s:%s:%s", config, group, resultType, asnNumber)
	}

	unwrapCache.Delete(key)
}

// 删除缓存结果
func (s *Store) DeleteCacheResult(keyType, config, group, key1, key2 string) {
	var cachePrefix string
	var dbPrefix string

	if key1 == "" && key2 == "" {
		cachePrefix = FormatCacheKey(keyType, config, group, "")
		dbPrefix = FormatDBKey("smart", keyType, config, group, "")
		RemoveCacheValuesByPrefix(cachePrefix)
		s.DeleteByPath(dbPrefix)
		return
	}

	if key2 != "" {
		cachePrefix = FormatCacheKey(keyType, config, group, key1, key2)
		dbPrefix = FormatDBKey("smart", keyType, config, group, key1, key2)
		DeleteCacheValue(cachePrefix)
		s.DeleteByPath(dbPrefix)
		return
	}

	cachePrefix = FormatCacheKey(keyType, config, group, key1)
	dbPrefix = FormatDBKey("smart", keyType, config, group, key1)
	RemoveCacheValuesByPrefix(cachePrefix)
	s.DeleteByPath(dbPrefix)
}

// 调整缓存参数
func (s *Store) AdjustCacheParameters() {
	memoryUsagePercent := GetSystemMemoryUsage()
	memoryUsage := memoryUsagePercent / 100.0

	smartGroupCount := 0
	for _, proxy := range tunnel.Proxies() {
		if proxy.Type() == C.Smart {
			smartGroupCount++
		}
	}

	globalCacheParams.mutex.Lock()

	isFirstRun := globalCacheParams.LastMemoryUsage == 0
	needAdjust := isFirstRun

	if !isFirstRun {
		memoryChanged := math.Abs(memoryUsage-globalCacheParams.LastMemoryUsage) > 0.1
		needAdjust = memoryChanged || memoryUsage > 0.7
	}

	globalCacheParams.LastMemoryUsage = memoryUsage

	if !needAdjust && !isFirstRun {
		globalCacheParams.mutex.Unlock()
		return
	}

	var newCacheSize int
	var cacheMaxAge int64 = CacheMaxAge

	if memoryUsage > 0.9 {
		globalCacheParams.MaxDomains = MinDomainsLimit
		globalCacheParams.BatchSaveThreshold = MinBatchThreshLimit
		globalCacheParams.PrefetchLimit = MinPrefetchDomainsLimit
		globalCacheParams.CacheMaxSize = (globalCacheParams.MaxDomains + globalCacheParams.PrefetchLimit) * smartGroupCount
		newCacheSize = globalCacheParams.CacheMaxSize / 2
		cacheMaxAge = CacheMaxAge / 2
	} else {
		adjustFactor := 4 * memoryUsage * (1 - memoryUsage)

		if memoryUsage > 0.85 {
			globalCacheParams.MaxDomains = MinDomainsLimit
			globalCacheParams.BatchSaveThreshold = MinBatchThreshLimit
			globalCacheParams.PrefetchLimit = MinPrefetchDomainsLimit
			globalCacheParams.CacheMaxSize = (globalCacheParams.MaxDomains + globalCacheParams.PrefetchLimit) * smartGroupCount
		} else {
			value := MinDomainsLimit + int(float64(MaxDomainsLimit-MinDomainsLimit)*adjustFactor*MemoryDomainsFactor)
			globalCacheParams.MaxDomains = ClampValue(value, MinDomainsLimit, MaxDomainsLimit)

			value = MinBatchThreshLimit + int(float64(MaxBatchThreshLimit-MinBatchThreshLimit)*adjustFactor*MemoryBatchFactor)
			globalCacheParams.BatchSaveThreshold = ClampValue(value, MinBatchThreshLimit, MaxBatchThreshLimit)

			value = MinPrefetchDomainsLimit + int(float64(MaxPrefetchDomainsLimit-MinPrefetchDomainsLimit)*adjustFactor*MemoryPrefetchFactor)
			globalCacheParams.PrefetchLimit = ClampValue(value, MinPrefetchDomainsLimit, MaxPrefetchDomainsLimit)

			globalCacheParams.CacheMaxSize = (globalCacheParams.MaxDomains + globalCacheParams.PrefetchLimit) * smartGroupCount
		}

		newCacheSize = globalCacheParams.CacheMaxSize
	}

	log.Infoln("[SmartStore] Parameters adjusted: MaxDomains=%d, CacheSize=%d, BatchThreshold=%d, PrefetchLimit=%d",
		globalCacheParams.MaxDomains, globalCacheParams.CacheMaxSize,
		globalCacheParams.BatchSaveThreshold, globalCacheParams.PrefetchLimit)

	globalCacheParams.mutex.Unlock()

	newDataCache := lru.New[string, interface{}](
		lru.WithSize[string, interface{}](newCacheSize),
		lru.WithAge[string, interface{}](cacheMaxAge),
	)

	domainCache = lru.New[string, string](
		lru.WithSize[string, string](globalCacheParams.MaxDomains),
		lru.WithAge[string, string](cacheMaxAge),
	)

	prefixCountCache = lru.New[string, int](
		lru.WithSize[string, int](2000),
		lru.WithAge[string, int](300),
	)

	nodeStatesCache = lru.New[string, map[string][]byte](
		lru.WithSize[string, map[string][]byte](2000),
		lru.WithAge[string, map[string][]byte](60),
	)

	unwrapCache = lru.New[string, []C.Proxy](
		lru.WithSize[string, []C.Proxy](500),
		lru.WithAge[string, []C.Proxy](300),
	)

	recordCache = lru.New[string, *AtomicStatsRecord](
		lru.WithSize[string, *AtomicStatsRecord](globalCacheParams.MaxDomains/2),
		lru.WithAge[string, *AtomicStatsRecord](CacheMaxAge),
	)

	var entries map[string]interface{}
	var preserveRatio float64

	if dataCache != nil {
		switch {
		case memoryUsage > 0.9:
			preserveRatio = 0.2
			entries = GetCacheValuesByPrefix(KeyTypeNode + ":")
			prefetchEntries := GetCacheValuesByPrefix(KeyTypePrefetch + ":")
			for k, v := range prefetchEntries {
				entries[k] = v
			}
		case memoryUsage > 0.8:
			preserveRatio = 0.4
			entries = GetCacheValuesByPrefix(KeyTypeNode + ":")
			prefetchEntries := GetCacheValuesByPrefix(KeyTypePrefetch + ":")
			for k, v := range prefetchEntries {
				entries[k] = v
			}
		case memoryUsage > 0.7:
			preserveRatio = 0.6
			entries = GetCacheValuesByPrefix("")
		default:
			preserveRatio = 0.8
			entries = GetCacheValuesByPrefix("")
		}

		dataCount := 0
		maxItems := int(float64(len(entries)) * preserveRatio)

		// 优先级顺序: 节点状态 > 预计算结果 > 统计数据 > 其他
		for k, v := range entries {
			if dataCount >= maxItems {
				break
			}

			if strings.HasPrefix(k, KeyTypeNode) ||
				strings.HasPrefix(k, KeyTypePrefetch) ||
				strings.HasPrefix(k, KeyTypeStats) ||
				strings.HasPrefix(k, KeyTypeRanking) {

				if bv, ok := v.([]byte); ok && len(bv) > 0 {
					newDataCache.Set(k, bv)
					dataCount++
				}
				continue
			}

			if dataCount < maxItems {
				newDataCache.Set(k, v)
				dataCount++
			}
		}

		log.Infoln("[SmartStore] Cache adjusted: preserved %d/%d items (%.1f%%) under memory pressure %.1f%%",
			dataCount, len(entries), float64(dataCount)/float64(len(entries))*100, memoryUsagePercent)
	}

	globalCacheLock.Lock()
	dataCache = newDataCache
	globalCacheLock.Unlock()

	queueLength := len(getGlobalQueueSnapshot())

	if (memoryUsage > 0.8 && queueLength > 0) ||
		(memoryUsage > 0.6 && queueLength > globalCacheParams.BatchSaveThreshold/2) {
		go s.FlushQueue(true)
	}
}

// 预加载数据
func (s *Store) PreloadFrequentData(group, config string) {
	log.Infoln("[SmartStore] Starting data preloading for group [%s], config [%s]", group, config)

	globalCacheParams.mutex.RLock()
	domainLimit := globalCacheParams.MaxDomains / 2
	prefetchLoadLimit := globalCacheParams.PrefetchLimit
	globalCacheParams.mutex.RUnlock()

	start := time.Now()

	prefetchCount := s.LoadAllPrefetchResults(group, config, prefetchLoadLimit)

	stateData, err := s.GetNodeStates(group, config)
	nodeStatesCount := 0
	if err == nil {
		nodeStatesCount = len(stateData)
	}

	ranking, _ := s.GetNodeWeightRankingCache(group, config)

	domains := s.GetActiveDomains(group, config, domainLimit)

	log.Infoln("[SmartStore] Preloaded data for group [%s]: %d domains, %d node stats, %d prefetch results, %d node rankings, completed in %.2f seconds",
		group, len(domains), nodeStatesCount, prefetchCount, len(ranking), time.Since(start).Seconds())
}

// 按级别清理内存缓存
func ClearCacheByLevel(level string, config string, group string) {
	if level == "all" {
		RemoveCacheValuesByPrefix("")
	} else if level == "config" {
		RemoveCacheValuesByPrefix(FormatCacheKey(keyTypeNetwork, config, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeFailed, config, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeNode, config, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeStats, config, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeRanking, config, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypePrefetch, config, ""))
	} else if level == "group" {
		RemoveCacheValuesByPrefix(FormatCacheKey(keyTypeNetwork, config, group))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeFailed, config, group, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeNode, config, group, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeStats, config, group, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeRanking, config, group, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypePrefetch, config, group, ""))
	}

	domainCache.Clear()

	prefixCountCache.Clear()

	nodeStatesCache.Clear()

	unwrapCache.Clear()

	recordCache.Clear()
}

// 从数据库路径提取缓存键
func ExtractCachePrefixFromPath(pathStr string) string {
	pathParts := strings.Split(pathStr, "/")

	if len(pathParts) >= 3 && pathParts[0] == "smart" {
		keyType := pathParts[1]
		config := pathParts[2]
		group := ""
		if len(pathParts) >= 4 {
			group = pathParts[3]
		}

		if len(pathParts) >= 6 && keyType == KeyTypeStats {
			// smart/stats/config/group/domain/node
			return FormatCacheKey(keyType, config, group, pathParts[4], pathParts[5])
		} else if len(pathParts) >= 5 && keyType != KeyTypeStats {
			// smart/keytype/config/group/target
			return FormatCacheKey(keyType, config, group, pathParts[4])
		} else if len(pathParts) == 4 {
			// smart/keytype/config/group
			return FormatCacheKey(keyType, config, group)
		} else if len(pathParts) == 3 {
			// smart/keytype/config
			return FormatCacheKey(keyType, config, "")
		}
	}

	return ""
}
