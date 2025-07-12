// This file is part of the Mihomo project: https://github.com/vernesong/mihomo
// Copyright (C) 2025 vernesong and contributors
//
// This file is licensed under the GNU General Public License v3.0.
// You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.html

package smart

import (
	"math"
	"time"
)

// CalculateWeight 计算权重
func CalculateWeight(success, failure, connectTime, latency int64, isUDP bool, uploadTotal, downloadTotal, connectionDuration float64, lastConnectTimestamp int64) float64 {
	// 1. 检查样本是否足够
	total := success + failure
	if total < DefaultMinSampleCount {
		return 0
	}

	// 2. 基础数据准备并分析场景
	uploadMB := uploadTotal / (1024 * 1024)
	downloadMB := downloadTotal / (1024 * 1024)
	durationMinutes := connectionDuration / 60000

	// 3. 场景识别和参数获取
	sceneType := identifyConnectionScene(isUDP, latency, uploadMB, downloadMB, durationMinutes, connectTime)

	var params SceneParams
	if p, ok := presetSceneParams[sceneType]; ok {
		params = p
	} else {
		params = presetSceneParams["web"]
	}

	// 4. 计算时间衰减因子
	timeFactor := 1.0
	if lastConnectTimestamp > 0 {
		simpleCache := make(map[int64]float64, 1)
		timeFactor = GetTimeDecayWithCache(lastConnectTimestamp, time.Now().Unix(), params.minDecayFactor, simpleCache)
	}

	// 5. 对所有历史数据应用时间衰减
	decayedSuccess := float64(success) * timeFactor
	decayedFailure := float64(failure) * timeFactor
	decayedConnectTime := float64(connectTime) * timeFactor
	decayedLatency := float64(latency) * timeFactor
	decayedTotal := decayedSuccess + decayedFailure

	if decayedTotal < 1.0 {
		decayedSuccess = math.Max(0.5, decayedSuccess)
		decayedFailure = math.Max(0.5, decayedFailure)
		decayedTotal = decayedSuccess + decayedFailure
	}

	if decayedConnectTime < 1.0 {
		decayedConnectTime = 1.0
	}
	if decayedLatency < 1.0 {
		decayedLatency = 1.0
	}

	// 6. 基础指标计算
	successRate := decayedSuccess / decayedTotal
	connectScore := 1.0 / decayedConnectTime
	latencyScore := 1.0 / decayedLatency

	// 7. UDP协议调整
	if isUDP {
		params.latencyWeight = math.Min(0.5, params.latencyWeight*1.2)
		params.successRateWeight = math.Min(0.6, params.successRateWeight*1.1)
		params.connectTimeWeight = 1.0 - params.successRateWeight - params.latencyWeight
	}

	// 8. 连接类型判断
	isShortConnection := connectionDuration <= 60000
	isLongConnection := connectionDuration > 600000

	// 9. 基础权重计算
	baseWeight := (successRate * params.successRateWeight) +
		(connectScore * params.connectTimeWeight) +
		(latencyScore * params.latencyWeight)

	// 10. 流量因子计算
	var trafficFactor float64 = 0
	if uploadMB > 0 || downloadMB > 0 {
		uploadFactor := calculateTrafficFactor(uploadMB, durationMinutes, isShortConnection)
		downloadFactor := calculateTrafficFactor(downloadMB, durationMinutes, isShortConnection)

		// 根据场景调整上下行权重
		var uploadWeight, downloadWeight float64
		if sceneType == "streaming" {
			uploadWeight, downloadWeight = 0.2, 0.8
		} else if sceneType == "transfer" && uploadMB > downloadMB*2 {
			uploadWeight, downloadWeight = 0.7, 0.3
		} else {
			uploadWeight, downloadWeight = 0.4, 0.6
		}

		trafficFactor = (uploadFactor * uploadWeight) + (downloadFactor * downloadWeight)
	}

	// 11. 持续时间因子计算
	var durationFactor float64 = 0.1
	if durationMinutes > 0 {
		if isShortConnection {
			durationFactor = math.Min(0.3, 0.1+math.Log1p(durationMinutes)*0.08)
		} else if isLongConnection {
			durationFactor = math.Min(0.5, 0.2+math.Log1p(durationMinutes)*0.1)
		} else {
			durationFactor = math.Min(0.4, 0.15+math.Log1p(durationMinutes)*0.09)
		}
	}

	// 12. 质量加成计算
	var qualityBonus float64 = 0

	if latency < 30 {
		qualityBonus += 0.1
	}
	if successRate > 0.95 {
		qualityBonus += 0.1
	}
	if (sceneType == "streaming" || sceneType == "transfer") && downloadMB > 20 {
		qualityBonus += 0.1
	}
	if sceneType == "interactive" && latency < 50 && successRate > 0.9 {
		qualityBonus += 0.1
	}

	qualityBonus = math.Min(0.3, qualityBonus)

	return baseWeight * (1 +
		trafficFactor*params.trafficWeight +
		durationFactor*params.durationWeight +
		qualityBonus*params.qualityWeight)
}

// 识别连接的使用场景类型
func identifyConnectionScene(isUDP bool, latency int64, uploadMB, downloadMB, durationMinutes float64, connectTime int64) string {
	const (
		SceneInteractive = "interactive" // gaming和voicevideo
		SceneStreaming   = "streaming"   // 流媒体
		SceneTransfer    = "transfer"    // filetransfer和大流量场景
		SceneWeb         = "web"         // browsing和api
	)

	// 游戏/互动场景特征：低延迟，持续连接，流量相对平衡
	if (isUDP && latency < 150 && durationMinutes > 3 &&
		uploadMB > 0.2 && downloadMB > 0.2) ||
		(!isUDP && latency < 250 && durationMinutes > 3 &&
			uploadMB > 0.1 && downloadMB > 0.1 &&
			uploadMB < 150 && downloadMB < 150 &&
			(uploadMB/downloadMB > 0.2) && (uploadMB/downloadMB < 5)) {
		return SceneInteractive
	}

	// 大流量传输场景 - 适应多CDN环境，单连接流量阈值降低
	if (uploadMB > 100 || downloadMB > 100) && durationMinutes > 0.5 {
		return SceneTransfer
	}

	// 流媒体场景
	if durationMinutes > 1 {
		// 高清/4K视频流
		if downloadMB > 60 && downloadMB/uploadMB > 3 {
			return SceneStreaming
		}

		// 标准流媒体
		if downloadMB > 15 && downloadMB/uploadMB > 3 {
			return SceneStreaming
		}
	}

	// 默认为Web场景
	return SceneWeb
}

// 计算流量因子
func calculateTrafficFactor(trafficMB, durationMinutes float64, isShort bool) float64 {
	if trafficMB <= 0 || durationMinutes <= 0 {
		return 0.0
	}

	throughput := trafficMB / math.Max(1.0, durationMinutes)

	var baseFactor float64
	switch {
	case trafficMB < 1:
		baseFactor = trafficMB * 0.7

	case trafficMB < 10:
		baseFactor = 0.7 + 0.3*math.Log10(trafficMB)

	case trafficMB < 50:
		baseFactor = 1.0 + 0.3*math.Log10(trafficMB/10)

	case trafficMB < 500:
		baseFactor = 1.3 + 0.3*math.Log10(trafficMB/50)

	case trafficMB < 3000:
		baseFactor = 1.6 + 0.3*math.Log10(trafficMB/500)

	default:
		baseFactor = 1.9 + 0.25*math.Log10(trafficMB/3000)
	}

	if durationMinutes < 0.5 && throughput > 80 {
		baseFactor *= 1.2
	} else if durationMinutes < 2 && throughput > 40 {
		baseFactor *= 1.1
	}

	var connectionFactor float64
	if isShort {
		if throughput > 50 {
			connectionFactor = 0.95
		} else if throughput > 10 {
			connectionFactor = 0.9
		} else {
			connectionFactor = 0.8
		}
	} else {
		connectionFactor = 1.0

		if throughput > 300 {
			baseFactor *= 1.5
		} else if throughput > 150 {
			baseFactor *= 1.4
		} else if throughput > 75 {
			baseFactor *= 1.3
		} else if throughput > 40 {
			baseFactor *= 1.2
		} else if throughput > 20 {
			baseFactor *= 1.1
		}

		// 特殊加成：高突发吞吐量场景
		if durationMinutes < 2 && throughput > 100 {
			baseFactor *= 1.15
		}
	}

	factor := baseFactor * connectionFactor

	if isShort {
		return math.Min(0.8, factor)
	}

	return math.Min(1.4, factor)
}
